#include <stdio.h>

int main(){
    
    printf("Starting CPU workload...\n");
    volatile unsigned long long i = 0ULL;
    while(1){
        i++;
    }
    return 0;

}