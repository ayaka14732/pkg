/*-
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ayaka <ayaka@mail.shn.hk>
 */

#include <sys/types.h>
#include <sys/wait.h>

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pkg.h"
#include "private/event.h"
#include "private/pkg.h"
#include "private/fetch.h"
#include "private/pkg_jobs.h"

/*
 * Fetch packages concurrently, one forked worker process per package.
 *
 * libfetch and the per-repository environment are process-global, so the
 * workers are processes rather than threads.  Each worker runs the ordinary
 * pkg_repo_fetch_package(), including mirrors, retries, resuming and checksum
 * verification, and forwards its events to the parent through a pipe.  Only
 * the parent calls the application callback, plugins and EVENT_PIPE.
 *
 * Only binary HTTP(S) repositories are handled: the SSH and TCP transports
 * keep a live connection which cannot be shared between processes.
 */

/* Upper bound for a string forwarded by a worker. */
#define FETCH_EVENT_MAX	(1024 * 1024)

/* A record on the event pipe; two NUL terminated strings follow it. */
struct fetch_message {
	pkg_event_t type;
	int value;
	size_t first;
	size_t second;
};

struct fetch_worker {
	pid_t pid;		/* 0 when the slot is free */
	struct pkg *pkg;
};

/* Signals that cancel the whole fetch. */
static const int fetch_signals[] = { SIGHUP, SIGINT, SIGTERM };

struct fetch_sigstate {
	sigset_t mask;		/* the signals handled by fetch_interrupted() */
	sigset_t oldmask;
	struct sigaction old[NELEM(fetch_signals)];
	bool installed[NELEM(fetch_signals)];
};

struct fetch_pool {
	struct fetch_worker *workers;
	struct pollfd *fds;	/* read end of each worker's event pipe */
	unsigned nworkers;
	struct fetch_sigstate sig;
};

static volatile sig_atomic_t fetch_signal;

static void
fetch_interrupted(int sig)
{
	fetch_signal = sig;
}

/*
 * Catch the cancellation signals, except those the caller ignores, so that
 * e.g. SIGHUP under nohup(1) does not abort the fetch.
 */
static int
fetch_signals_install(struct fetch_sigstate *ss)
{
	struct sigaction sa = { .sa_handler = fetch_interrupted };
	sigset_t all;
	size_t i;
	int ret = 0;

	fetch_signal = 0;
	sigemptyset(&sa.sa_mask);
	sigemptyset(&ss->mask);
	sigemptyset(&all);
	for (i = 0; i < NELEM(fetch_signals); i++)
		sigaddset(&all, fetch_signals[i]);

	/* A signal arriving meanwhile is delivered to the final handler. */
	sigprocmask(SIG_BLOCK, &all, &ss->oldmask);
	for (i = 0; i < NELEM(fetch_signals); i++) {
		ss->installed[i] = false;
		if (sigaction(fetch_signals[i], NULL, &ss->old[i]) == -1) {
			ret = -1;
			break;
		}
		if ((ss->old[i].sa_flags & SA_SIGINFO) == 0 &&
		    ss->old[i].sa_handler == SIG_IGN)
			continue;
		if (sigaction(fetch_signals[i], &sa, NULL) == -1) {
			ret = -1;
			break;
		}
		ss->installed[i] = true;
		sigaddset(&ss->mask, fetch_signals[i]);
	}
	if (ret == -1) {
		for (i = 0; i < NELEM(fetch_signals); i++) {
			if (ss->installed[i])
				sigaction(fetch_signals[i], &ss->old[i], NULL);
		}
	}
	sigprocmask(SIG_SETMASK, &ss->oldmask, NULL);
	return (ret);
}

/*
 * Restore the caller's handlers and redeliver a signal caught meanwhile.
 * Return that signal if the process survives it, otherwise 0.
 */
static int
fetch_signals_restore(struct fetch_sigstate *ss)
{
	size_t i;
	int sig;

	sigprocmask(SIG_BLOCK, &ss->mask, NULL);
	sig = fetch_signal;
	for (i = 0; i < NELEM(fetch_signals); i++) {
		if (ss->installed[i])
			sigaction(fetch_signals[i], &ss->old[i], NULL);
	}
	if (sig != 0)
		raise(sig);
	sigprocmask(SIG_SETMASK, &ss->oldmask, NULL);
	return (sig);
}

/* Worker side: write a whole record, or give up on the parent. */
static void
worker_write(int fd, const void *data, size_t len)
{
	const char *p = data;
	ssize_t n;

	while (len > 0) {
		n = write(fd, p, len);
		if (n == -1 && errno == EINTR)
			continue;
		if (n <= 0)
			_exit(EPKG_FATAL);
		p += n;
		len -= n;
	}
}

/* Worker side event callback: forward the events the parent reports. */
static int
worker_event(void *data, struct pkg_event *ev)
{
	struct fetch_message msg = { .type = ev->type };
	const char *first = "", *second = "";
	int fd = *(int *)data;

	switch (ev->type) {
	case PKG_EVENT_ERROR:
		first = ev->e_pkg_error.msg;
		break;
	case PKG_EVENT_NOTICE:
		first = ev->e_pkg_notice.msg;
		break;
	case PKG_EVENT_DEBUG:
		first = ev->e_debug.msg;
		msg.value = ev->e_debug.level;
		break;
	case PKG_EVENT_ERRNO:
	case PKG_EVENT_PKG_ERRNO:
		first = ev->e_errno.func;
		second = ev->e_errno.arg;
		msg.value = ev->e_errno.no;
		break;
	case PKG_EVENT_FETCH_BEGIN:
	case PKG_EVENT_FETCH_FINISHED:
		first = ev->e_fetching.url;
		break;
	case PKG_EVENT_PKG_FETCH_BEGIN:
		break;
	default:
		/* The parent reports progress for the whole batch. */
		return (0);
	}
	if (first == NULL)
		first = "";
	if (second == NULL)
		second = "";
	msg.first = strlen(first) + 1;
	msg.second = strlen(second) + 1;
	worker_write(fd, &msg, sizeof(msg));
	worker_write(fd, first, msg.first);
	worker_write(fd, second, msg.second);
	return (0);
}

static void
worker_main(struct pkg *pkg, int fd, const struct fetch_sigstate *ss)
{
	size_t i;
	int ret;

	/*
	 * The parent's handlers would cancel the parent's state; ignored
	 * signals stay ignored.  SIGPIPE keeps its disposition: pkg ignores
	 * it so that a dropped connection is reported as a fetch error.
	 */
	for (i = 0; i < NELEM(fetch_signals); i++) {
		if (ss->installed[i])
			signal(fetch_signals[i], SIG_DFL);
	}
	sigprocmask(SIG_SETMASK, &ss->oldmask, NULL);

	pkg_event_register_worker(worker_event, &fd);
	ret = pkg_repo_fetch_package(pkg);
	/* Skip atexit handlers and stdio buffers inherited from the parent. */
	_exit(ret);
}

/* Read a whole record: 1 on success, 0 at EOF, -1 on error or signal. */
static int
fetch_read(int fd, void *data, size_t len)
{
	char *p = data;
	size_t off = 0;
	ssize_t n;

	while (off < len && fetch_signal == 0) {
		n = read(fd, p + off, len - off);
		if (n == -1 && errno == EINTR)
			continue;
		if (n == 0)
			return (off == 0 ? 0 : -1);
		if (n < 0)
			return (-1);
		off += n;
	}
	return (off == len ? 1 : -1);
}

/* Emit the next event of a worker: 1 on success, 0 at EOF, -1 on error. */
static int
fetch_forward_event(int fd, struct pkg *pkg)
{
	struct fetch_message msg;
	struct pkg_event ev = { 0 };
	char *first, *second;
	int ret;

	ret = fetch_read(fd, &msg, sizeof(msg));
	if (ret != 1)
		return (ret);
	if (msg.first == 0 || msg.second == 0 ||
	    msg.first > FETCH_EVENT_MAX || msg.second > FETCH_EVENT_MAX)
		return (-1);
	first = xmalloc(msg.first + msg.second);
	second = first + msg.first;
	ret = fetch_read(fd, first, msg.first + msg.second);
	if (ret != 1 || first[msg.first - 1] != '\0' ||
	    second[msg.second - 1] != '\0') {
		free(first);
		return (-1);
	}

	ev.type = msg.type;
	switch (msg.type) {
	case PKG_EVENT_ERROR:
		ev.e_pkg_error.msg = first;
		break;
	case PKG_EVENT_NOTICE:
		ev.e_pkg_notice.msg = first;
		break;
	case PKG_EVENT_DEBUG:
		ev.e_debug.msg = first;
		ev.e_debug.level = msg.value;
		break;
	case PKG_EVENT_ERRNO:
	case PKG_EVENT_PKG_ERRNO:
		ev.e_errno.func = first;
		ev.e_errno.arg = second;
		ev.e_errno.no = msg.value;
		break;
	case PKG_EVENT_FETCH_BEGIN:
	case PKG_EVENT_FETCH_FINISHED:
		ev.e_fetching.url = first;
		break;
	case PKG_EVENT_PKG_FETCH_BEGIN:
		ev.e_pkg_fetching.pkg = pkg;
		break;
	default:
		free(first);
		return (-1);
	}
	pkg_emit_event(&ev);
	free(first);
	return (1);
}

static int
fetch_worker_start(struct fetch_pool *pool, unsigned slot, struct pkg *pkg)
{
	int fd[2];
	pid_t pid;
	unsigned i;

	if (pipe(fd) == -1) {
		pkg_emit_errno("pipe", "fetch worker");
		return (EPKG_FATAL);
	}
	/* Hold cancellation until the worker has reset its handlers. */
	sigprocmask(SIG_BLOCK, &pool->sig.mask, NULL);
	pid = fork();
	if (pid == 0) {
		close(fd[0]);
		for (i = 0; i < pool->nworkers; i++) {
			if (pool->fds[i].fd != -1)
				close(pool->fds[i].fd);
		}
		worker_main(pkg, fd[1], &pool->sig);
	}
	sigprocmask(SIG_SETMASK, &pool->sig.oldmask, NULL);
	/* Only the worker may hold the write end, so that its exit is EOF. */
	close(fd[1]);
	if (pid == -1) {
		pkg_emit_errno("fork", "fetch worker");
		close(fd[0]);
		return (EPKG_FATAL);
	}
	pool->workers[slot].pid = pid;
	pool->workers[slot].pkg = pkg;
	pool->fds[slot].fd = fd[0];
	pool->fds[slot].events = POLLIN;
	return (EPKG_OK);
}

/*
 * Collect a worker whose event pipe reached EOF, and return the result of
 * its fetch.
 */
static int
fetch_worker_reap(struct fetch_pool *pool, unsigned slot)
{
	struct fetch_worker *w = &pool->workers[slot];
	int ret, status;
	pid_t pid;

	do {
		pid = waitpid(w->pid, &status, 0);
	} while (pid == -1 && errno == EINTR && fetch_signal == 0);
	if (pid == -1) {
		/* fetch_workers_stop() reaps it after a signal. */
		if (fetch_signal == 0)
			pkg_emit_errno("waitpid", "fetch worker");
		return (EPKG_FATAL);
	}
	w->pid = 0;
	close(pool->fds[slot].fd);
	pool->fds[slot].fd = -1;
	ret = WIFEXITED(status) ? WEXITSTATUS(status) : EPKG_FATAL;
	if (ret == EPKG_OK)
		return (EPKG_OK);
	/* EPKG_END would make the caller fetch everything again. */
	if (ret == EPKG_END)
		ret = EPKG_FATAL;
	/*
	 * The worker reported the cause, but without a "Fetching" status
	 * line for each package, it does not always name the package.
	 */
	if (WIFSIGNALED(status))
		pkg_emit_error("Failed to fetch %s-%s: "
		    "worker killed by signal %d", w->pkg->name,
		    w->pkg->version, WTERMSIG(status));
	else
		pkg_emit_error("Failed to fetch %s-%s", w->pkg->name,
		    w->pkg->version);
	return (ret);
}

/* Kill before reaping: a worker may be blocked writing to its pipe. */
static void
fetch_workers_stop(struct fetch_pool *pool)
{
	unsigned i;

	for (i = 0; i < pool->nworkers; i++) {
		if (pool->workers[i].pid != 0)
			kill(pool->workers[i].pid, SIGKILL);
	}
	for (i = 0; i < pool->nworkers; i++) {
		if (pool->workers[i].pid != 0) {
			while (waitpid(pool->workers[i].pid, NULL, 0) == -1 &&
			    errno == EINTR)
				;
			pool->workers[i].pid = 0;
		}
		if (pool->fds[i].fd != -1) {
			close(pool->fds[i].fd);
			pool->fds[i].fd = -1;
		}
	}
}

static int
fetch_workers_run(struct fetch_pool *pool, struct pkg **queue, size_t count)
{
	size_t next = 0, done = 0;
	unsigned i;
	int ret;

	while (done < count) {
		for (i = 0; i < pool->nworkers && next < count; i++) {
			if (fetch_signal != 0)
				return (EPKG_FATAL);
			if (pool->workers[i].pid != 0)
				continue;
			if (fetch_worker_start(pool, i, queue[next]) != EPKG_OK)
				return (EPKG_FATAL);
			next++;
		}
		/* The timeout bounds the delay of a signal caught early. */
		if (poll(pool->fds, pool->nworkers, 250) == -1) {
			if (errno == EINTR)
				continue;
			pkg_emit_errno("poll", "fetch workers");
			return (EPKG_FATAL);
		}
		for (i = 0; i < pool->nworkers; i++) {
			if (fetch_signal != 0)
				return (EPKG_FATAL);
			if (pool->fds[i].fd == -1 || pool->fds[i].revents == 0)
				continue;
			ret = fetch_forward_event(pool->fds[i].fd,
			    pool->workers[i].pkg);
			if (ret == 1)
				continue;
			if (ret == -1) {
				if (fetch_signal == 0)
					pkg_emit_error("Failed to fetch %s-%s: "
					    "invalid event from worker",
					    pool->workers[i].pkg->name,
					    pool->workers[i].pkg->version);
				return (EPKG_FATAL);
			}
			ret = fetch_worker_reap(pool, i);
			if (ret != EPKG_OK)
				return (ret);
			pkg_emit_progress_tick(++done, count);
		}
	}
	return (EPKG_OK);
}

static bool
fetch_parallel_supported(struct pkg *pkg)
{
	const char *url;

	if (pkg->repo == NULL || strcmp(pkg->repo->ops->type, "binary") != 0)
		return (false);
	url = pkg_repo_url(pkg->repo);
	if (url == NULL)
		return (false);
	if (strncasecmp(url, "pkg+", 4) == 0)
		url += 4;
	return (strncasecmp(url, "http://", 7) == 0 ||
	    strncasecmp(url, "https://", 8) == 0);
}

/*
 * Collect the packages to fetch.  Return false if the job needs the
 * sequential path: an unsupported transport, a name-version shared by two
 * packages (they share the cache symlink), or too few packages to gain.
 */
static bool
fetch_parallel_queue(struct pkg_jobs *j, struct pkg ***queuep, size_t *countp)
{
	struct pkg_solved *ps;
	struct pkg **queue, *pkg;
	stringset_t *names;
	size_t count = 0;
	char *name;
	bool ok = true;

	queue = xcalloc(j->jobs.len, sizeof(*queue));
	names = stringset_new();
	vec_foreach(j->jobs, i) {
		ps = j->jobs.d[i];
		if (ps->type == PKG_SOLVED_DELETE ||
		    ps->type == PKG_SOLVED_UPGRADE_REMOVE)
			continue;
		pkg = ps->items[0]->pkg;
		if (pkg->type != PKG_REMOTE)
			continue;
		if (!fetch_parallel_supported(pkg)) {
			ok = false;
			break;
		}
		pkg_asprintf(&name, "%n-%v", pkg, pkg);
		ok = stringset_add(names, name);
		free(name);
		if (!ok)
			break;
		queue[count++] = pkg;
	}
	stringset_destroy(names);
	if (!ok || count < 2) {
		free(queue);
		return (false);
	}
	*queuep = queue;
	*countp = count;
	return (true);
}

/*
 * Resolve the servers of each repository once, here, rather than once per
 * package in the workers.
 */
static void
fetch_parallel_prepare(struct pkg **queue, size_t count)
{
	struct pkg_repo **repos;
	size_t i, j, nrepos = 0;

	repos = xcalloc(count, sizeof(*repos));
	for (i = 0; i < count; i++) {
		for (j = 0; j < nrepos; j++) {
			if (repos[j] == queue[i]->repo)
				break;
		}
		if (j < nrepos)
			continue;
		repos[nrepos++] = queue[i]->repo;
		pkg_fetch_prepare(queue[i]->repo);
	}
	free(repos);
}

/* Return EPKG_END if the caller has to fetch sequentially instead. */
int
pkg_jobs_fetch_parallel(struct pkg_jobs *j, unsigned nworkers)
{
	struct fetch_pool pool = { 0 };
	struct pkg_event ev = { .type = PKG_EVENT_FETCH_MULTI_BEGIN };
	struct pkg **queue;
	size_t count;
	unsigned i;
	int ret;

	if (!fetch_parallel_queue(j, &queue, &count))
		return (EPKG_END);

	pool.nworkers = MIN(nworkers, count);
	pool.workers = xcalloc(pool.nworkers, sizeof(*pool.workers));
	pool.fds = xcalloc(pool.nworkers, sizeof(*pool.fds));
	for (i = 0; i < pool.nworkers; i++)
		pool.fds[i].fd = -1;

	fetch_parallel_prepare(queue, count);
	if (fetch_signals_install(&pool.sig) == -1) {
		pkg_emit_errno("sigaction", "fetch workers");
		ret = EPKG_FATAL;
		goto out;
	}
	/* Never let workers share a cached HTTP or TLS connection. */
	libfetch_flush_connections();

	pkg_emit_event(&ev);
	pkg_emit_progress_start("Fetching packages (%u workers)",
	    pool.nworkers);
	ret = fetch_workers_run(&pool, queue, count);
	fetch_workers_stop(&pool);
	ev.type = PKG_EVENT_FETCH_MULTI_FINISHED;
	pkg_emit_event(&ev);

	if (fetch_signals_restore(&pool.sig) != 0) {
		pkg_emit_error("Fetching packages interrupted");
		ret = EPKG_FATAL;
	}
out:
	free(pool.workers);
	free(pool.fds);
	free(queue);
	return (ret);
}
