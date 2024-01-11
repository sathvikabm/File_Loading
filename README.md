# Multithreaded File Reader

## Files
| FileName | Description | Compilation | Usage |
|----------|----------|----------|----------|
|priority.c | Source code for priority chunk loading of file | `gcc -fopenmp priority.c -o priority` |`./priority`|
|gen_input.c | Helper file for generating random txt file | `gcc gen_input.c common.c -o gen_input` |`./gen_input <file_size_GB> <num_copies>`|
|random_csv_generator.c | Helper file for generating random CSV file | `gcc random_csv_generator.c -o random_csv_generator` |`./random_csv_generator`|
