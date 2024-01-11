#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define FILE_SIZE_GB 1
#define BUFFER_SIZE 1024

int main()
{
    FILE *file;
    file = fopen("random_file.csv", "w");
    if (file == NULL)
    {
        perror("Error opening file");
        return 1;
    }

    srand((unsigned int)time(NULL));
    long long targetSize = FILE_SIZE_GB * 1024LL * 1024LL * 1024LL; // 1GB
    long long currentSize = 0;
    char buffer[BUFFER_SIZE];
    int num, length;

    while (currentSize < targetSize)
    {
        length = 0; // Reset buffer length for each line
        // Fill buffer with random numbers and commas
        while (length < BUFFER_SIZE - 12)
        { // 12 is the max length of an int when printed
            num = rand();
            length += snprintf(buffer + length, BUFFER_SIZE - length, "%d,", num);
        }
        // Replace last comma with a newline
        buffer[length - 1] = '\n';
        fputs(buffer, file);
        currentSize += length;
    }

    fclose(file);
    printf("CSV file created successfully for %d GB.\n", FILE_SIZE_GB);
    return 0;
}
