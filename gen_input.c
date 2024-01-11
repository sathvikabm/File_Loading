#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <string.h>

#define BUFFER_SIZE 1024
#define FILENAME "random_file.txt"

void create_new_random_file(double file_size_gb){
    FILE *file;
    char buffer[BUFFER_SIZE];
    srand(time(NULL));

    file = fopen(FILENAME, "wb");

    if (file == NULL) {
        perror("Error opening file");
        return;
    }

    while (ftell(file) < file_size_gb * 1024L* 1024L* 1024L ) {
        for (int i = 0; i < BUFFER_SIZE; i++) {
            buffer[i] = 'A' + rand() % 26;  // Random characters (0 to 255)
        }

        fwrite(buffer, sizeof(char), BUFFER_SIZE, file);
    }

    fclose(file);

    printf("File created successfully for %fGB.\n", file_size_gb);

}

void copy_file(const char *source_filename, const char *dest_filename) {
    FILE *source, *dest;
    char buffer[BUFFER_SIZE];
    size_t bytes;

    // Open source file for reading
    source = fopen(source_filename, "rb");
    if (source == NULL) {
        perror("Error opening source file");
        exit(EXIT_FAILURE);
    }

    // Open destination file for writing
    dest = fopen(dest_filename, "wb");
    if (dest == NULL) {
        perror("Error opening destination file");
        fclose(source);  // Close source file before exiting
        exit(EXIT_FAILURE);
    }

    // Copy the file contents
    while ((bytes = fread(buffer, sizeof(char), BUFFER_SIZE, source)) > 0) {
        fwrite(buffer, sizeof(char), bytes, dest);
    }

    // Close the files
    fclose(source);
    fclose(dest);
}

void create_copies(int num){
    for(int i=0;i<num;i++){
        char dest_filename[10];
        snprintf(dest_filename, sizeof(dest_filename), "copy%d.txt", i);
        copy_file(FILENAME, dest_filename);
        printf("Created copy: %s\n", dest_filename);
    }
}

int main(int argc, char *argv[]){
    // usage: ./gen_input <file_size_GB> <num_copies>
    if(argc!=3){
        printf("Usage: ./gen_input <file_size_GB> <num_copies>\n");
        return 0;
    }

    srand((unsigned int)10);

    char *endptr;
    double file_size_gb = strtod(argv[1], &endptr);

    if (endptr == argv[1]) {
        printf("Not able to convert input size to double.\n");
        exit(EXIT_FAILURE);
    }

    printf("Creating file of size: %f\n", file_size_gb);
    create_new_random_file(file_size_gb);
    create_copies(atoi(argv[2]));

    return 0;
}