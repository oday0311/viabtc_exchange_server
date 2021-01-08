/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/16, create
 */

# include "me_config.h"
# include "me_operlog.h"
# include "me_market.h"
# include "me_balance.h"
# include "me_update.h"
# include "me_trade.h"
# include "me_persist.h"
# include "me_history.h"
# include "me_message.h"
# include "me_cli.h"
# include "me_server.h"

const char *__process__ = "matchengine";
const char *__version__ = "0.1.0";


//
//matchengine: This is the most important part
//for it records user balance and executes user order.
//It is in memory database, saves operation log in MySQL
//and redoes the operation log when start. It also writes user history into MySQL,
//push balance, orders and deals message to kafka.




nw_timer cron_timer;

static void on_cron_check(nw_timer *timer, void *data)
{
    dlog_check_all();
    if (signal_exit) {
        nw_loop_break();
        signal_exit = 0;
    }
}

static int init_process(void)
{
    if (settings.process.file_limit) {
        if (set_file_limit(settings.process.file_limit) < 0) {
            return -__LINE__;
        }
    }
    if (settings.process.core_limit) {
        if (set_core_limit(settings.process.core_limit) < 0) {
            return -__LINE__;
        }
    }

    return 0;
}

static int init_log(void)
{
    default_dlog = dlog_init(settings.log.path, settings.log.shift, settings.log.max, settings.log.num, settings.log.keep);
    if (default_dlog == NULL)
        return -__LINE__;
    default_dlog_flag = dlog_read_flag(settings.log.flag);
    if (alert_init(&settings.alert) < 0)
        return -__LINE__;

    return 0;
}

int main(int argc, char *argv[])
{
    printf("process: %s version: %s, compile date: %s %s\n", __process__, __version__, __DATE__, __TIME__);

    if (argc < 2) {
        printf("usage: %s config.json\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    if (process_exist(__process__) != 0) {
        printf("process: %s exist\n", __process__);
        exit(EXIT_FAILURE);
    }

    int ret;

    //高精度有关的类
    ret = init_mpd();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init mpd fail: %d", ret);
    }
    //配置文件
    ret = init_config(argv[1]);
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "load config fail: %d", ret);
    }
    //初始化进程配置
    ret = init_process();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init process fail: %d", ret);
    }
    //初始化日志有关
    ret = init_log();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init log fail: %d", ret);
    }

    //初始化资产有关， balance， dict_t 是一个内存级的hash表， balance， update，和trade的主要数据都加载到内存里， 提高交易匹配速度。
    //高速一定是把关键数据组织到内存里的
    ret = init_balance();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init balance fail: %d", ret);
    }

    //初始化资产有关的对象， 这个和balance类似， 不过是有时间有效性的？
    ret = init_update();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init update fail: %d", ret);
    }
    ret = init_trade();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init trade fail: %d", ret);
    }

    daemon(1, 1);
//    daemon(1, 1)； //参数根据需求确定
//
//    /*  在这里添加你需要在后台做的工作代码  */
    process_keepalive();

    //从数据库中获取数据
    ret = init_from_db();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init from db fail: %d", ret);
    }

    //从数据库中获取操作日志
    ret = init_operlog();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init oper log fail: %d", ret);
    }


    ret = init_history();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init history fail: %d", ret);
    }
    //初始化kafka 消息对象
    ret = init_message();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init message fail: %d", ret);
    }

    //定时持久化数据
    ret = init_persist();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init persist fail: %d", ret);
    }

    //初始化命令交互
    ret = init_cli();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init cli fail: %d", ret);
    }
    ret = init_server();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init server fail: %d", ret);
    }

    nw_timer_set(&cron_timer, 0.5, true, on_cron_check, NULL);
    nw_timer_start(&cron_timer);

    log_vip("server start");
    log_stderr("server start");
    nw_loop_run();
    log_vip("server stop");

    fini_message();
    fini_history();
    fini_operlog();

    return 0;
}

