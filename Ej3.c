//
// Created by levixhu on 28/02/24.
//

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include "compress.h"
#include "chunk_archive.h"
#include "queue.h"
#include "options.h"

#define CHUNK_SIZE (1024*1024)
#define QUEUE_SIZE 20

#define COMPRESS 1
#define DECOMPRESS 0

struct thread_args {
    pthread_t id;
    queue in;
    queue out;
};

struct read_args {
    pthread_t idRead;
    int chunks;
    int size;
    int fd;
    queue in;
};

struct write_args {
    pthread_t idWrite;
    queue out;
    archive ar;
};

// Worker function for compressing chunks concurrently
void worker(queue in, queue out) {
    chunk ch, res;
    while (q_elements(in) > 0 || q_canInsert(in) != true) { //aun no es true
        ch = q_remove(in);
        if(ch == 0) break;
        res = zcompress(ch);
        free_chunk(ch);
        q_insert(out, res);
    }
}

void *compress_worker(void *args) {
    struct thread_args *t_args = (struct thread_args *)args;
    queue in = t_args->in;
    queue out = t_args->out;

    worker(in, out);

    pthread_exit(NULL);
}

void *reader(void *args) {
    struct read_args *r_args = (struct read_args *)args;
    int i, offset;

    for (i = 0; i < r_args->chunks; i++) {
        chunk ch = alloc_chunk(r_args->size);
        offset = lseek(r_args->fd, 0, SEEK_CUR);
        ch->size = read(r_args->fd, ch->data, r_args->size);
        ch->num = i;
        ch->offset = offset;

        q_insert(r_args->in, ch);
    }

    pthread_exit(NULL);
}

void *writer(void *args) {
    struct write_args *w_args = (struct write_args *)args;
    archive ar = w_args->ar;
    queue out = w_args->out;

    while (1) {
        chunk ch = q_remove(out);
        if (ch == NULL) {
            break;
        }
        add_chunk(ar, ch);
        free_chunk(ch);
    }

    close_archive_file(ar);

    pthread_exit(NULL);
}

// Compress file taking chunks of opt.size from the input file,
// inserting them into the in queue, running them using multiple workers,
// and sending the output from the out queue into the archive file
void comp(struct options opt) {
    int fd, chunks, i;
    char comp_file[256];
    struct stat st;
    archive ar;
    queue in, out;

    if ((fd = open(opt.file, O_RDONLY)) == -1) {
        printf("Cannot open %s\n", opt.file);
        exit(0);
    }

    fstat(fd, &st);
    chunks = st.st_size / opt.size + (st.st_size % opt.size ? 1 : 0);

    if (opt.out_file) {
        strncpy(comp_file, opt.out_file, 255);
    } else {
        strncpy(comp_file, opt.file, 255);
        strncat(comp_file, ".ch", 255);
    }

    ar = create_archive_file(comp_file);

    in = q_create(opt.queue_size);
    out = q_create(opt.queue_size);

    // Create thread for reading
    struct read_args readArgs;
    readArgs.size = opt.size;
    readArgs.in = in;
    readArgs.fd = fd;
    readArgs.chunks = chunks;

    pthread_create(&(readArgs.idRead), NULL, reader, (void *)&readArgs);

    // Create threads for compressing
    struct thread_args t_args[opt.num_threads];
    for (i = 0; i < opt.num_threads; i++) {
        t_args[i].in = in;
        t_args[i].out = out;
        pthread_create(&(t_args[i].id), NULL, compress_worker, (void *)&t_args[i]);
    }

    // Create thread for writing
    struct write_args writeArgs;
    writeArgs.out = out;
    writeArgs.ar = ar;

    pthread_create(&(writeArgs.idWrite), NULL, writer, (void *)&writeArgs);

    // Join threads
    pthread_join(readArgs.idRead, NULL);
    q_setReady(&in, true);
    q_setReady(&readArgs.in, true);

    for (i = 0; i < opt.num_threads; i++) {
        pthread_join(t_args[i].id, NULL);
    }
    q_setReady(&out, true);
    q_setReady(&writeArgs.out, true);
    pthread_join(writeArgs.idWrite, NULL);

    close(fd);
    q_destroy(in);
    q_destroy(out);
}

// Decompress file taking chunks of size opt.size from the input file
void decomp(struct options opt) {
    int fd, i;
    char uncomp_file[256];
    archive ar;
    chunk ch, res;

    if((ar=open_archive_file(opt.file))==NULL) {
        printf("Cannot open archive file\n");
        exit(0);
    };

    if(opt.out_file) {
        strncpy(uncomp_file, opt.out_file, 255);
    } else {
        strncpy(uncomp_file, opt.file, strlen(opt.file) -3);
        uncomp_file[strlen(opt.file)-3] = '\0';
    }

    if((fd=open(uncomp_file, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH))== -1) {
        printf("Cannot create %s: %s\n", uncomp_file, strerror(errno));
        exit(0);
    }

    for(i=0; i<chunks(ar); i++) {
        ch = get_chunk(ar, i);

        res = zdecompress(ch);
        free_chunk(ch);

        lseek(fd, res->offset, SEEK_SET);
        write(fd, res->data, res->size);
        free_chunk(res);
    }

    close_archive_file(ar);
    close(fd);
}

int main(int argc, char *argv[]) {
    struct options opt;

    opt.compress = COMPRESS;
    opt.num_threads = 3;
    opt.size = CHUNK_SIZE;
    opt.queue_size = QUEUE_SIZE;
    opt.out_file = NULL;

    read_options(argc, argv, &opt);

    if (opt.compress == COMPRESS)
        comp(opt);
    else
        decomp(opt);

    return 0;
}
