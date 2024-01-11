#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <omp.h>

#define NUM_CHUNKS 100000
#define NUM_PRIORITIES 100
#define NUM_THREADS 16

// Chunk struct
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

typedef struct
{
    Chunk **chunks; // Array of pointers to chunks
    int numChunks;  // Number of chunks in the batch
} Batch;

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
    int a = 0;
    for (int i = 0; i < 50000; i++)
    {
        a++;
    }
    // Process the chunk of the CSV file here
    int lines = 0;
    for (size_t i = 0; i < bytesRead; ++i)
    {
        if (buffer[i] == '\n')
        {
            lines++;
        }
    }
    printf("Processed : with %d priority and ChunkID is %d.\n", priority, chunkID);
}
void groupChunksByPriority(PriorityQueue pq[NUM_PRIORITIES], Batch batches[NUM_PRIORITIES])
{
    for (int i = 0; i < NUM_PRIORITIES; i++)
    {
        int count = 0;
        Node *current = pq[i].head;

        // Count the number of chunks in the current priority
        while (current != NULL)
        {
            count++;
            current = current->next;
        }

        // Allocate space for the batch
        batches[i].chunks = (Chunk **)malloc(count * sizeof(Chunk *));
        batches[i].numChunks = count;
        current = pq[i].head;
        // Fill the batch with chunk pointers
        for (int j = 0; j < count; j++)
        {
            batches[i].chunks[j] = &current->data;
            current = current->next;
        }
    }
}

void generate_priorities(FILE *file, PriorityQueue pq[NUM_PRIORITIES])
{
    // Calculate file size
    fseek(file, 0, SEEK_END);
    long fileSize = ftell(file);
    fseek(file, 0, SEEK_SET);

    // Calculate chunk size
    long chunkSize = fileSize / NUM_CHUNKS;

    // Calculate how many chunks per priority
    int chunksPerPriority = NUM_CHUNKS / NUM_PRIORITIES;

    for (int i = 0; i < NUM_CHUNKS; ++i)
    {
        // Handling the size of the last chunk
        long actualChunkSize = (i == NUM_CHUNKS - 1) ? (fileSize - chunkSize * i) : chunkSize;

        Chunk newChunk;
        newChunk.chunkID = i;

        // Priority calculation with bounds check
        int priority = (i / chunksPerPriority) + 1;
        if (priority > NUM_PRIORITIES)
        {
            priority = NUM_PRIORITIES;
        }

        newChunk.priority = priority;
        // Allocate memory for the chunk's buffer
        newChunk.buffer = (char *)malloc(actualChunkSize);

        // Read file data into the chunk's buffer
        newChunk.bytesRead = fread(newChunk.buffer, 1, actualChunkSize, file);

        pq_push(&pq[priority - 1], newChunk); // Push into the respective priority queue
    }
}
int main()
{
    FILE *file = fopen("random_file.csv", "r");
    if (file == NULL)
    {
        printf("Error opening file.\n");
        return 1;
    }

    PriorityQueue pq[NUM_PRIORITIES];
    for (int i = 0; i < NUM_PRIORITIES; i++)
    {
        pq_init(&pq[i]);
    }

    generate_priorities(file, pq);

    // Declare batches array here
    Batch batches[NUM_PRIORITIES];
    groupChunksByPriority(pq, batches);

    double start_time = omp_get_wtime();

    // Sequentially process each priority level
    for (int i = 0; i < NUM_PRIORITIES; i++)
    {
        Batch batch = batches[i];

// Parallelize processing of chunks within the same priority
#pragma omp parallel for num_threads(NUM_THREADS)
        for (int j = 0; j < batch.numChunks; j++)
        {
            Chunk *chunk = batch.chunks[j];
            if (chunk && chunk->chunkID != -1)
            {
                processChunk(chunk->buffer, chunk->bytesRead, chunk->priority, chunk->chunkID);
                free(chunk->buffer);
            }
        }
    }

    double end_time = omp_get_wtime();
    printf("Total parallel time taken: %f seconds\n", end_time - start_time);

    fclose(file);
    return 0;
}
