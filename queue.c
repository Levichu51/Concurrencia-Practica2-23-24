#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>

// circular array
typedef struct _queue {
    int size;
    int used;
    int first;
    void **data;
    pthread_mutex_t mutex;
    pthread_cond_t full;
    pthread_cond_t empty;
    bool ready;
} _queue;

#include "queue.h"

//transformar insert y remove

queue q_create(int size) {
    queue q = malloc(sizeof(_queue));

    if(q == NULL)
        return NULL;

    q->size  = size;
    q->used  = 0;
    q->first = 0;
    q->data  = malloc(size*sizeof(void *));
    q->ready = false;

    if(q->data == NULL) {
        free(q);
        return NULL;
    }

    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->full, NULL);
    pthread_cond_init(&q->empty, NULL);

    return q;
}

int q_elements(queue q) {
    return q->used;
}

int q_insert(queue q, void *elem) {
    pthread_mutex_lock(&q->mutex);

    while (q->used == q->size) {
        pthread_cond_wait(&q->full, &q->mutex);
    }

    q->data[(q->first + q->used) % q->size] = elem;
    q->used++;

    if (q->used == 1) {
        pthread_cond_broadcast(&q->empty); // Signal that the queue is no longer empty
    }
    pthread_mutex_unlock(&q->mutex);

    return 1;
}

void *q_remove(queue q) {
    pthread_mutex_lock(&q->mutex);

    while (q->used == 0 && !q->ready) {
        pthread_cond_wait(&q->empty, &q->mutex);
    }

    if (q->used == 0 && q->ready) {
        pthread_mutex_unlock(&q->mutex);
        return NULL;
    }

    void *res;

    res = q->data[q->first];
    q->first = (q->first + 1) % q->size;
    q->used--;

    if (q->used == q->size - 1) {
        pthread_cond_broadcast(&q->full); // Signal that the queue is no longer full
    }

    pthread_mutex_unlock(&q->mutex);

    return res;
}

void q_destroy(queue q) {
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->full);
    pthread_cond_destroy(&q->empty);
    free(q->data);
    free(q);
}

void q_setReady(queue *q, bool foo){
    pthread_mutex_lock(&(*q)->mutex);
    (*q)->ready = foo;

    if(foo == true){
        pthread_cond_broadcast(&(*q)->empty); //ojo con esto
    }
    else{
        pthread_cond_broadcast(&(*q)->full);
    }
    pthread_mutex_unlock(&(*q)->mutex);
}

bool q_canInsert(queue q){
    return q->ready;
}
