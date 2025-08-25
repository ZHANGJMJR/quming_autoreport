
from email.mime.application import MIMEApplication
import pyodbc
import configparser
from datetime import datetime
from pyexcelerate import Workbook
import os
import logging
from logging.handlers import TimedRotatingFileHandler
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
# 定时任务相关库
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

# 初始化日志配置
def init_logging():
    # 创建logs目录（如果不存在）
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # 日志文件格式：logs/2025-08-25.log
    log_filename = os.path.join('logs', f"{datetime.now().strftime('%Y-%m-%d')}.log")

    # 创建日志处理器，按天滚动
    handler = TimedRotatingFileHandler(
        log_filename,
        when='midnight',  # 每天午夜滚动
        interval=1,  # 间隔1天
        backupCount=30,  # 保留30天日志
        encoding='utf-8'
    )

    # 日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)

    # 获取根日志器并配置
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger

# 读取邮件配置
def read_email_config(config_file='config.ini'):
    logger = logging.getLogger()
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')
    if 'Email' not in config.sections():
        logger.error("配置文件中未找到 Email 段")
        raise Exception("配置文件中未找到 Email 段")
    logger.info("成功读取邮件配置")
    return {
        'smtp_server': config.get('Email', 'smtp_server'),
        'port': config.getint('Email', 'port'),
        'sender': config.get('Email', 'sender'),
        'password': config.get('Email', 'password'),
        'receivers': [r.strip() for r in config.get('Email', 'receivers').split(',')]
    }

def read_schedule_config(config_file='config.ini'):
    logger = logging.getLogger()
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')
    if 'Schedule' not in config.sections():
        logger.error("配置文件中未找到 Schedule 段")
        raise Exception("配置文件中未找到 Schedule 段")
    logger.info("成功读取定时配置")
    return {
       'cron_expression': config.get('Schedule', 'cron_expression', fallback='0 2 * * *')}


# 读取数据库配置
def read_db_config(config_file='config.ini'):
    logger = logging.getLogger()
    try:
        config = configparser.ConfigParser()
        config.read(config_file, encoding='utf-8')
        if 'Database' not in config.sections():
            logger.error("配置文件中未找到Database配置段")
            raise Exception("配置文件中未找到Database配置段")
        logger.info("成功读取数据库配置")
        return {
            'server': config.get('Database', 'server'),
            'database': config.get('Database', 'database'),
            'username': config.get('Database', 'username'),
            'password': config.get('Database', 'password'),
            'driver': config.get('Database', 'driver', fallback='{SQL Server Native Client 10.0}')
        }
    except Exception as e:
        logger.error(f"读取配置文件失败: {str(e)}")
        raise


# 快速保存到Excel
def save_to_excel_fast(data, filename):
    logger = logging.getLogger()
    if not data:
        logger.info("没有数据可写入 Excel")
        return

    try:
        # 列名
        columns = list(data[0].keys())
        # 数据内容
        rows = [list(d.values()) for d in data]

        wb = Workbook()
        ws = wb.new_sheet("Sheet1", data=[columns] + rows)
        wb.save(filename)
        logger.info(f"快速写入完成: {filename}")
    except Exception as e:
        logger.error(f"写入Excel失败: {str(e)}")


# 获取报表数据
def get_baobiao_gongfei_result():
    logger = logging.getLogger()
    conn = None
    try:
        db_config = read_db_config()
        conn = pyodbc.connect(
            f"DRIVER={db_config['driver']};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            f"UID={db_config['username']};"
            f"PWD={db_config['password']};"
            "TDS_Version=8.0"
        )
        cursor = conn.cursor()
        logger.info("成功连接数据库并创建游标")

        cursor.execute("EXEC [dbo].[aa_baobiao_gongfei] ?, ?, ?", None, None, None)
        logger.info("已执行存储过程 [dbo].[aa_baobiao_gongfei]")

        rows = []
        columns = []
        while True:
            if cursor.description:  # 找到第一个有效结果集
                rows = cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                logger.info(f"成功获取结果集，共 {len(rows)} 条数据")
                break
            if not cursor.nextset():
                break

        results = [dict(zip(columns, row)) for row in rows] if rows else []
        return results

    except pyodbc.Error as e:
        logger.error(f"数据库错误: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"执行错误: {str(e)}")
        return None
    finally:
        if conn:
            conn.close()
            logger.info("数据库连接已关闭")




# ============ 发送邮件 ============
def send_mail_with_attachment(filename):
    try:
        mail_config = read_email_config()

        msg = MIMEMultipart()
        msg['From'] = mail_config['sender']
        msg['To'] = ",".join(mail_config['receivers'])
        msg['Subject'] = filename

        body = MIMEText("请查收附件：智导回款分析台账-工费报表", 'plain', 'utf-8')
        msg.attach(body)

        with open(filename, 'rb') as f:
            attachment = MIMEApplication(f.read(), Name=os.path.basename(filename))
        attachment['Content-Disposition'] = f'attachment; filename="{os.path.basename(filename)}"'
        msg.attach(attachment)

        if mail_config['port'] == 465:
            server = smtplib.SMTP_SSL(mail_config['smtp_server'], mail_config['port'])
            server.login(mail_config['sender'], mail_config['password'])
            server.sendmail(mail_config['sender'], mail_config['receivers'], msg.as_string())
        else:
            server = smtplib.SMTP(mail_config['smtp_server'], mail_config['port'])
            server.starttls()
            server.login(mail_config['username'], mail_config['password'])
            server.sendmail(mail_config['sender'], mail_config['receivers'], msg.as_string())

        logging.info(f"邮件发送成功，附件: {filename}，收件人: {mail_config['receivers']}")
        return True

    except Exception as e:
        logging.error(f"邮件发送失败: {e}")
        return False

#定时任务事件监听（错误处理）
def job_listener(event):
    logger = logging.getLogger()
    if event.exception:
        logger.error(f"定时任务执行失败: {str(event.exception)}", exc_info=event.exception)
    else:
        logger.info("定时任务执行成功（来自监听器）")

def mainjob():
    try:
        data = get_baobiao_gongfei_result()
        if data:
            logger.info(f"成功获取 {len(data)} 条数据")
            # 日志中记录前2条数据示例
            for i, row in enumerate(data[:2]):
                logger.info(f"第{i + 1}条数据示例: {str(row)}")

            filename = f"智导回款分析台账合并合计-工费-{datetime.now().strftime('%Y-%m-%d')}.xlsx"
            save_to_excel_fast(data, filename)
            send_mail_with_attachment(filename)
        else:
            logger.warning("未能获取数据")
    except Exception as e:
        logger.error(f"mainjob执行出错: {str(e)}", exc_info=True)
    finally:
        logger.info("mainjob执行结束")


if __name__ == "__main__":
    # 初始化日志
    logger = init_logging()
    logger.info("程序开始执行")
    mainjob()
    try:
        # 读取配置
        schedule_config = read_schedule_config()
        cron_expr = schedule_config['cron_expression']
        # 初始化调度器
        scheduler = BackgroundScheduler(timezone='Asia/Shanghai')  # 使用上海时区
        # 添加任务
        scheduler.add_job(
            mainjob,
            trigger=CronTrigger.from_crontab(cron_expr),
            name="报表生成与发送邮件",
            misfire_grace_time=120  # 允许5分钟的执行延迟
        )

        # 添加事件监听器
        scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

        # 启动调度器
        scheduler.start()
        logger.info(f"定时任务已启动，执行表达式: {cron_expr}")
        logger.info("程序将持续运行，按Ctrl+C终止...")

        # 保持程序运行
        while True:
            time.sleep(60)  # 每分钟检查一次

    except Exception as e:
        logger.error(f"初始化失败：{str(e)}", exc_info=True)
    finally:
        # 优雅关闭
        if 'scheduler' in locals() and scheduler.running:
            scheduler.shutdown()
            logger.info("定时任务已关闭")
        logger.info("程序退出")
        for handler in logging.getLogger().handlers:
            handler.close()
