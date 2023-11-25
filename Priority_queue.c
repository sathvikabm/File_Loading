#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <time.h>

#define NUM_THREADS 8
#define CHUNK_SIZE (1024 * 1024) // 1MB chunk size

typedef struct
{
    int chunkID;
    char *buffer;
    size_t bytesRead;
    int priority;
} Chunk;

// Define a node for the priority queue
typedef struct node
{
    Chunk data;
    struct node *next;
} Node;

// Define the priority queue
typedef struct
{
    Node *head;
} PriorityQueue;

void pq_init(PriorityQueue *pq)
{
    pq->head = NULL;
}

int pq_is_empty(PriorityQueue *pq)
{
    return pq->head == NULL;
}

void pq_push(PriorityQueue *pq, Chunk data)
{
    Node *newNode = (Node *)malloc(sizeof(Node));
    newNode->data = data;
    newNode->next = NULL;

    // Insert new node based on priority
    if (pq->head == NULL || data.priority < pq->head->data.priority)
    {
        newNode->next = pq->head;
        pq->head = newNode;
    }
    else
    {
        Node *current = pq->head;
        while (current->next != NULL && current->next->data.priority <= data.priority)
        {
            current = current->next;
        }
        newNode->next = current->next;
        current->next = newNode;
    }
}

Chunk pq_pop(PriorityQueue *pq)
{
    if (pq_is_empty(pq))
    {
        Chunk empty;
        empty.chunkID = -1; // Indicate empty
        return empty;
    }

    Node *temp = pq->head;
    Chunk data = temp->data;
    pq->head = pq->head->next;
    free(temp);
    return data;
}

void processChunk(char *buffer, size_t bytesRead, int priority, int chunkID)
{
    // Process the chunk of the CSV file here
    int lines = 0;
    for (size_t i = 0; i < bytesRead; ++i)
    {
        if (buffer[i] == '\n')
        {
            lines++;
        }
    }
    printf("Processed %d lines with %d priority and ChunkID is %d.\n", lines, priority, chunkID);
}

void generateRandomPrioritiesAndReadFile(PriorityQueue *pq)
{
    FILE *file = fopen("genres_v2.csv", "r");
    if (file == NULL)
    {
        perror("Error opening file");
        exit(1);
    }

    fseek(file, 0, SEEK_END);
    long fileSize = ftell(file);
    fseek(file, 0, SEEK_SET);
    int totalChunks = fileSize / CHUNK_SIZE + (fileSize % CHUNK_SIZE != 0);

    srand(time(NULL));
    for (int i = 0; i < totalChunks; ++i)
    {
        size_t sizeToRead = (i < totalChunks - 1) ? CHUNK_SIZE : (fileSize - i * CHUNK_SIZE);
        char *buffer = (char *)malloc(sizeToRead);
        if (buffer == NULL)
        {
            perror("Failed to allocate buffer");
            exit(1);
        }

        size_t bytesRead = fread(buffer, 1, sizeToRead, file);
        if (bytesRead > 0)
        {
            Chunk chunk;
            chunk.chunkID = i;
            chunk.buffer = buffer;
            chunk.bytesRead = bytesRead;
            chunk.priority = rand() % (totalChunks * 10); // Larger range for priorities
            pq_push(pq, chunk);
        }
        else
        {
            free(buffer);
        }
    }

    fclose(file);
}

int main()
{
    PriorityQueue pq;
    pq_init(&pq);

    generateRandomPrioritiesAndReadFile(&pq);

    double start = omp_get_wtime();

#pragma omp parallel num_threads(NUM_THREADS)
    {
        Chunk chunk;
        do
        {
            int hasChunk = 0;

#pragma omp critical
            {
                chunk = pq_pop(&pq);
                if (chunk.chunkID != -1)
                {
                    hasChunk = 1;
                }
            }

            if (hasChunk)
            {
                processChunk(chunk.buffer, chunk.bytesRead, chunk.priority, chunk.chunkID);
                free(chunk.buffer); // Free the buffer after processing
            }
        } while (chunk.chunkID != -1);
    }

    double end = omp_get_wtime();

    printf("Total time taken: %f seconds\n", end - start);

    // Optional: Clean up the priority queue
    while (!pq_is_empty(&pq)) {
        Chunk chunk = pq_pop(&pq);
        free(chunk.buffer);
    }

    return 0;
}
