#ifndef REDISCHAT_H
#define REDISCHAT_H

#ifdef __cplusplus
extern "C" {
#endif

int chat_init(void);
char *chat_get(const char *request);
char *chat_prepare(const char *response);


#ifdef __cplusplus
}
#endif

#endif
