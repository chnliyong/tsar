
/*
 * (C) 2010-2011 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


#include <fcntl.h>
#include <string.h>
#include <time.h>
#include "tsar.h"

void
send_data_tcp(char *output_addr, char *data, int len)
{
    int        fd, flags, res;
    fd_set     fdr, fdw;
    struct     timeval timeout;
    struct     sockaddr_in db_addr;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        do_debug(LOG_FATAL, "can't get socket");
    }

    /* set socket fd noblock */
    if ((flags = fcntl(fd, F_GETFL, 0)) < 0) {
        close(fd);
        return;
    }

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(fd);
        return;
    }

    /* get db server address */
    db_addr = *str2sa(output_addr);

    if (connect(fd, (struct sockaddr*)&db_addr, sizeof(db_addr)) != 0) {
        if (errno != EINPROGRESS) { // EINPROGRESS
            close(fd);
            return;

        } else {
            goto select;
        }

    } else {
        goto send;
    }

select:
    FD_ZERO(&fdr);
    FD_ZERO(&fdw);
    FD_SET(fd, &fdr);
    FD_SET(fd, &fdw);

    timeout.tv_sec = 2;
    timeout.tv_usec = 0;

    res = select(fd + 1, &fdr, &fdw, NULL, &timeout);
    if (res <= 0) {
        close(fd);
        return;
    }

send:
    if (len > 0 && write(fd, data, len) != len) {
        do_debug(LOG_ERR, "output_db write error:dst:%s\terrno:%s\n",output_addr, strerror(errno));
    }
    close(fd);
}


static void
get_instance_id(char *instance_id, int max_len) {
    FILE *fp;
    char data[1024 * 1024];
    int len;
    char *token;

    if ((fp = fopen("/etc/instance-info", "r")) != NULL) {
        len = (int)fread(data, 1, 4906, fp);
        fclose(fp);

        if (len > 0 && data[len-1] != '\0') {
            data[len-1] = '\0';
        }

        token = strtok(data, " \t\r\n=");
        while (token != NULL) {
            if (strcmp(token, "INSTANCE_ID") == 0) {
                token = strtok(NULL, " \t\r\n=");
                break;
            } else {
                token = strtok(NULL, " \t\r\n=");
            }
        }
        strncpy(instance_id, token, max_len);
    } else {
        strncpy(instance_id, "nil", max_len);
    }
}


/* 
 * The output data format is: <hostname>\ttsar\t<metric data>
 * This function currently add an instanceId and timestamp tag
 * for the record, the result is:
 *   <hostname>\ttsar/instanceId=<id>,timestamp=<unix time>\t<metric data>.
 */
static int
add_tags(char *data, int len, int max_len)
{
    char instance_id[128];
    char tags[256];
    int tags_len;
    char *p, *q;
    unsigned long long unix_time;

    data[len] = '\0';

    get_instance_id(instance_id, 128);

    unix_time = time(NULL);
    /* The cron job execute every 60 seconds, align the timestamp */
    unix_time = unix_time - (unix_time % 60ULL);

    sprintf(tags, "#%lld#instanceId=%s", unix_time, instance_id);
    tags_len = strlen(tags);

    p = strrchr(data, '\t');

    if (p != NULL && len + tags_len < max_len) {
        for (q = data + len + tags_len; q >= p + tags_len; q--) {
            *q = *(q - tags_len);
        }
        for (q = p; q < p + tags_len; q++) {
            *q = tags[q-p];
        }
        return len + tags_len;
    }
    return len;
}


void
output_multi_tcp(int have_collect)
{
    int           out_pipe[2];
    int           len;
    static char   data[LEN_10M] = {0};
    int         i;
    /* only output from output_tcp_mod */
    reload_modules(conf.output_tcp_mod);

    if (!strcasecmp(conf.output_tcp_merge, "on") || !strcasecmp(conf.output_tcp_merge, "enable")) {
        conf.print_merge = MERGE_ITEM;
    } else {
        conf.print_merge = MERGE_NOT;
    }

    if (pipe(out_pipe) != 0) {
        return;
    }

    dup2(out_pipe[1], STDOUT_FILENO);
    close(out_pipe[1]);

    running_check(RUN_CHECK_NEW);

    fflush(stdout);
    len = read(out_pipe[0], data, LEN_10M);
    close(out_pipe[0]);
    len = add_tags(data, len, LEN_10M);
    do_debug(LOG_DEBUG, data);
    /*now ,the data to send is gotten*/
    for(i = 0; i < conf.output_tcp_addr_num; i++){
        send_data_tcp(conf.output_tcp_addr[i], data, len);
    }
}
