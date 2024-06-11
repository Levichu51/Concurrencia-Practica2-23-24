//
// Created by levixhu on 23/02/24.
//

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>
#include "queue.h"

#define NUM_THREADS 5
#define NUM_ELEMENTS 10

queue q;

void *producer(void *arg) {
    int *thread_id = (int *)arg;

    for (int i = 0; i < NUM_ELEMENTS; ++i) {
        int *data = malloc(sizeof(int));
        *data = i;
        q_insert(q, data);
        printf("Producer %d inserted element: %d\n", *thread_id, *data);
        usleep(100000);
    }

    pthread_exit(NULL);
}

void *consumer(void *arg) {
    int *thread_id = (int *)arg;

    for (int i = 0; i < NUM_ELEMENTS; ++i) {
        int *data = q_remove(q);
        if (data != NULL) {
            printf("Consumer %d removed element: %d\n", *thread_id, *data);
            free(data);
        } else {
            printf("Consumer %d tried to remove from an empty queue.\n", *thread_id);
        }
        usleep(150000); // Sleep for 150 ms
    }

    pthread_exit(NULL);
}

int main() {
    q = q_create(20);

    pthread_t producers[NUM_THREADS];
    pthread_t consumers[NUM_THREADS];
    int thread_ids[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; ++i) {
        thread_ids[i] = i + 1;
        pthread_create(&producers[i], NULL, producer, &thread_ids[i]);
        pthread_create(&consumers[i], NULL, consumer, &thread_ids[i]);
    }

    for (int i = 0; i < NUM_THREADS; ++i) {
        pthread_join(producers[i], NULL);
        pthread_join(consumers[i], NULL);
    }

    q_destroy(q);

    return 0;
}