#include <stdio.h>
#include <stdlib.h>

#include "guidance.h"
guidance_t g_guidance;
int main() {
  guidance_t *g = &g_guidance;
  printf("g at %p, size: %zu\n", g, sizeof(guidance_t));
  printf("status at: %p, %p and %p\n", &g->cluster[0], &g->cluster[1],
         &g->cluster[2]);
}