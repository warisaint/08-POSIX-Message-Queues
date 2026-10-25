#include "msgq_lab.h"

#include <errno.h>
#include <fcntl.h>
#include <mqueue.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

/* One queue name per task — prevents cross-contamination between runs */
#define Q_EXCHANGE "/msgq_lab_exchange"
#define Q_PRIO     "/msgq_lab_prio"
#define Q_FULL     "/msgq_lab_full"
#define Q_SIZE     "/msgq_lab_size"

/*
 * mq_msgsize used when opening every queue.
 * Must be >= sizeof(struct SensorReading) so a struct fits in one message.
 */
#define MSG_BUF_SIZE 128

/*
 * Task A — task_struct_exchange()
 *
 * Demonstrates: passing a struct between two processes via a POSIX message
 * queue.  Unlike a pipe, the queue persists until explicitly unlinked.
 *
 * Steps:
 *   1. mq_unlink(Q_EXCHANGE) — remove any leftover queue (ignore errors).
 *   2. Open the queue with mq_open():
 *        flags  = O_CREAT | O_RDWR
 *        mode   = 0660
 *        attr   = { .mq_maxmsg = 4, .mq_msgsize = MSG_BUF_SIZE }
 *   3. Fork one child process.
 *   4. Parent:
 *        a. Build SensorReading r = { .sensor_id = 7, .value = 42 }.
 *        b. mq_send(mqd, (char *)&r, sizeof(r), 0) — priority 0.
 *        c. mq_close(mqd).
 *        d. waitpid() for the child.
 *        e. Print exactly:  parent: sent reading
 *   5. Child:
 *        a. mq_receive(mqd, buf, MSG_BUF_SIZE, &prio) — blocks until message.
 *        b. Cast buf to SensorReading * and print exactly:
 *             child received: sensor id=7 value=42
 *        c. mq_close(mqd).
 *        d. mq_unlink(Q_EXCHANGE).
 *        e. exit(0).
 *
 * Note: mq_receive buffer size must be >= mq_msgsize (use MSG_BUF_SIZE).
 */
int task_struct_exchange(void) {
    /* TODO: implement task A */
    struct mq_attr attr = {0};
        attr.mq_maxmsg = 4;
        attr.mq_msgsize = MSG_BUF_SIZE;


    mq_unlink(Q_EXCHANGE);

    mqd_t mqd = mq_open(Q_EXCHANGE, O_CREAT|O_RDWR, 0660, &attr);

    if (mqd == (mqd_t)-1) {
        perror("mq_open");
        return 1;
    }

    //fflush(stdout);
    pid_t pid = fork();

    if (pid < 0) {
        perror("fork");
        mq_close(mqd);
        mq_unlink(Q_EXCHANGE);
        return 1;
    }


    if (pid == 0) {
        char buffer [MSG_BUF_SIZE];
        unsigned int priority;

        if(mq_receive (mqd, buffer, sizeof(buffer), &priority) == -1) {
            perror("mq_receive");
            mq_close(mqd);
            _exit(1);
        }

        struct SensorReading *received = (struct SensorReading *)buffer;
        

        printf("child received: sensor id=%d value=%d\n", received->sensor_id, received->value);
        fflush(stdout);

        mq_close(mqd);
        _exit(0);
    }

    struct SensorReading r = {
            .sensor_id = 7,
            .value = 42
    };    

    if (mq_send(mqd, (const char *) &r, sizeof(r), 0) == -1) {
        perror("mq_send");
        mq_close(mqd);
        mq_unlink(Q_EXCHANGE);
        waitpid(pid, NULL, 0);
        return 1;
    }

    mq_close(mqd);

    int status;
    if (waitpid(pid, &status, 0) == -1) {
        perror("waitpid");
        return 1;
    }

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        return 1;
    }

    printf("parent: sent reading\n");
    fflush(stdout);
    return 0;

    /*(void)Q_EXCHANGE;
    fprintf(stderr, "TODO: task_struct_exchange not implemented\n");
    return 1;
    */
}

/*
 * Task B — task_priority_order()
 *
 * Demonstrates: POSIX message queues deliver messages highest-priority-first,
 * regardless of send order (key difference from pipes and System V queues with
 * type=0).
 *
 * Steps:
 *   1. mq_unlink(Q_PRIO) — clean slate.
 *   2. Open the queue O_CREAT | O_RDWR with mq_maxmsg=4, mq_msgsize=MSG_BUF_SIZE.
 *   3. Before forking, send TWO messages:
 *        a. SensorReading { .sensor_id = 1, .value = 10 } at priority 1  (first)
 *        b. SensorReading { .sensor_id = 2, .value = 99 } at priority 5  (second)
 *   4. mq_close(mqd), then fork().
 *   5. Child:
 *        a. Re-open the queue O_RDONLY (no O_CREAT).
 *        b. First mq_receive → highest priority wins → value=99.
 *           Print exactly:  child received hi-pri: value=99
 *        c. Second mq_receive → remaining message → value=10.
 *           Print exactly:  child received lo-pri: value=10
 *        d. mq_close, mq_unlink(Q_PRIO), exit(0).
 *   6. Parent: waitpid() for the child.
 *
 * Key insight: value=10 was enqueued first but is dequeued LAST because its
 * priority (1) is lower than priority (5).
 */
int task_priority_order(void) {
    /* TODO: implement task B */
    mq_unlink(Q_PRIO);
    struct mq_attr attr = {0};
    attr.mq_maxmsg = 4;
    attr.mq_msgsize = MSG_BUF_SIZE;

    mqd_t mqd = mq_open(Q_PRIO, O_CREAT | O_RDWR, 0660, &attr);

    if (mqd == (mqd_t)-1) {
        perror("mq_open");
        return 1;
    }

    struct SensorReading low = {
        .sensor_id = 1,
        .value = 10
    };

    if (mq_send(mqd, (const char *)&low, sizeof(low), 1) == -1) {
        perror("mq_send low priority");
        mq_close(mqd);
        mq_unlink(Q_PRIO);
        return 1;
    }

    struct SensorReading high = {
        .sensor_id = 2,
        .value = 99
    };

    if (mq_send(mqd, (const char *)&high, sizeof(high), 5) == -1) {
        perror("mq_send high");
        mq_close(mqd);
        mq_unlink(Q_PRIO);
        return 1;
    }

    if (mq_close(mqd) == -1) {
        perror("mq_close");
        mq_unlink(Q_PRIO);
        return 1;
    }

    pid_t pid = fork();

    if (pid == -1) {
        perror("fork");
        mq_unlink(Q_PRIO);
        return 1;
    }

    if (pid == 0) {
        mqd_t reader = mq_open(Q_PRIO, O_RDONLY);

        if (reader == (mqd_t)-1) {
            perror("mq_open child");
            _exit(1);
        }

        char buffer[MSG_BUF_SIZE];
        unsigned int prio;

        if (mq_receive(reader, buffer, sizeof(buffer), &prio) == -1) {
                perror("mq_receive high");
                mq_close(reader);
                _exit(1);
        }

        struct SensorReading *received = (struct SensorReading *)buffer;

        printf("child received hi-pri: value=%d\n", received->value);
        fflush(stdout);

        if (mq_receive(reader, buffer, sizeof(buffer), &prio) == -1) {
                perror("mq_receive low");
                mq_close(reader);
                _exit(1);
        }

        received = (struct SensorReading *)buffer;

        printf("child received lo-pri: value=%d\n", received->value);
        fflush(stdout);

        mq_close(reader);
        mq_unlink(Q_PRIO);
        _exit(0);
    }

    int status;
    if (waitpid(pid, &status, 0) == -1) {
        perror("waitpid");
        return 1;
    }

    if (!WIFEXITED (status) || WEXITSTATUS (status) != 0) {
        return 1;
    }

    return 0;
    /*(void)Q_PRIO;
    fprintf(stderr, "TODO: task_priority_order not implemented\n");
    return 1;
    */
}

/*
 * Task C — task_full_queue()
 *
 * Demonstrates: a bounded POSIX queue has a hard capacity of mq_maxmsg.
 * When the queue is full and O_NONBLOCK is set, mq_send() fails immediately
 * with errno == EAGAIN instead of blocking.
 *
 * Steps (no fork required):
 *   1. mq_unlink(Q_FULL) — clean slate.
 *   2. Open the queue with mq_maxmsg=2, mq_msgsize=MSG_BUF_SIZE.
 *      Include O_NONBLOCK in the flags.
 *   3. Send two messages (both succeed — queue is now full).
 *   4. Attempt a third mq_send().
 *      It must fail: errno == EAGAIN.
 *   5. If errno == EAGAIN, print exactly:  queue full: EAGAIN detected
 *   6. mq_close, mq_unlink(Q_FULL).
 */
int task_full_queue(void) {
    /* TODO: implement task C */
    mq_unlink(Q_FULL);
    struct mq_attr attr = {0};
    attr.mq_maxmsg = 2;
    attr.mq_msgsize = MSG_BUF_SIZE;

    mqd_t mqd = mq_open(Q_FULL, O_CREAT|O_WRONLY|O_NONBLOCK, 0660, &attr);

    struct SensorReading r = {
        .sensor_id = 1,
        .value = 10
    };

    for (int i = 0; i < 2; i++) {
        if (mq_send(mqd, (const char *)&r, sizeof(r), 0) == -1) {
            perror("mq_send");
            mq_close(mqd);
            mq_unlink(Q_FULL);
            return 1;
        }
    }

    if (mq_send(mqd, (const char *)&r, sizeof(r), 0) == -1) {
        if (errno == EAGAIN) {
            printf("queue full: EAGAIN detected\n");
        } else {
            perror("mq_send third message");
            mq_close(mqd);
            mq_unlink(Q_FULL);
            return 1;
        }
    } else {
        fprintf(stderr, "Expected queue to be full\n");
        mq_close(mqd);
        mq_unlink(Q_FULL);
        return 1;
    }

    mq_close(mqd);
    mq_unlink(Q_FULL);

    return 0;
    /*
    (void)Q_FULL;
    fprintf(stderr, "TODO: task_full_queue not implemented\n");
    return 1;
    */
}

/*
 * Task D — task_size_limit()
 *
 * Demonstrates: mq_send() enforces mq_msgsize; any payload whose byte length
 * exceeds mq_msgsize causes the call to return -1 with errno == EMSGSIZE.
 *
 * Steps (no fork required):
 *   1. mq_unlink(Q_SIZE) — clean slate.
 *   2. Open the queue with mq_msgsize = sizeof(struct SensorReading).
 *      Use O_CREAT | O_WRONLY.
 *   3. Prepare a char buffer of size sizeof(struct SensorReading) + 1.
 *   4. Call mq_send() with that buffer — it must fail: errno == EMSGSIZE.
 *   5. If errno == EMSGSIZE, print exactly:  oversized send: EMSGSIZE detected
 *   6. mq_close, mq_unlink(Q_SIZE).
 */
int task_size_limit(void) {
    /* TODO: implement task D */
    mq_unlink(Q_SIZE);
    struct mq_attr attr = {0};
    attr.mq_maxmsg = 4;
    attr.mq_msgsize = sizeof(struct SensorReading);
    
    mqd_t mqd = mq_open(Q_SIZE, O_CREAT|O_WRONLY, 0660, &attr);

    if (mqd == (mqd_t)-1) {
        perror("mq_open");
        mq_unlink(Q_SIZE);
        return 1;
    }

    char buffer[sizeof(struct SensorReading) + 1] = {'\0'};

    if (mq_send(mqd, buffer, sizeof(buffer), 0) == -1) {
        if (errno == EMSGSIZE) {
            printf("oversized send: EMSGSIZE detected\n");
        } else {
            perror("mq_send");
            mq_close(mqd);
            mq_unlink(Q_SIZE);
            return 1;
        }
    } else {
        fprintf(stderr, "Expected EMSGSIZE for oversized message\n");
        mq_close(mqd);
        mq_unlink(Q_SIZE);
        return 1;
    }

    mq_close(mqd);
    mq_unlink(Q_SIZE);

    return 0;

    /*(void)Q_SIZE;
    fprintf(stderr, "TODO: task_size_limit not implemented\n");
    return 1;
    */
}

/* ------------------------------------------------------------------ */
/* main — do not modify                                                */
/* ------------------------------------------------------------------ */
int main(void) {
    if (task_struct_exchange() != 0) return EXIT_FAILURE;
    if (task_priority_order()  != 0) return EXIT_FAILURE;
    if (task_full_queue()      != 0) return EXIT_FAILURE;
    if (task_size_limit()      != 0) return EXIT_FAILURE;
    printf("all tasks done\n");
    return EXIT_SUCCESS;
}
