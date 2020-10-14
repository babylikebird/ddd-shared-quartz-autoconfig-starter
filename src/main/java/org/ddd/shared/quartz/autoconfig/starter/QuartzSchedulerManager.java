package org.ddd.shared.quartz.autoconfig.starter;

import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.quartz.impl.triggers.CronTriggerImpl;

import javax.print.attribute.standard.JobName;
import java.text.ParseException;
import java.util.Date;

/**
 * @author Mr.Yangxiufeng
 * @date 2020-10-13
 * @time 14:24
 */
@Slf4j
public class QuartzSchedulerManager {

    /**
     * 默认的分组名称
     */
    private static final String DEFAULT_GROUP = "DEFAULT_GROUP";

    /**
     * 默认的分组名称
     */
    private static final String DEFAULT_JOB_GROUP = "DEFAULT_GROUP";

    /**
     * 默认触发器分组名称
     */
    private static final String DEFAULT_TRIGGER_GROUP_NAME = "DEFAULT_GROUP";

    private Scheduler scheduler;

    public QuartzSchedulerManager(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    /**
     * <p>添加一个任务</p>
     * @param jobClass jobClass
     * @param cronExpression cronExpression
     */
    public void  addJob(Class<? extends Job> jobClass, String cronExpression){
        String jobName = jobClass.getName();
        addJob(jobName, jobClass, cronExpression);
    }

    /**
     * 添加一个任务
     * @param jobName jobName
     * @param jobClass jobClass
     * @param cronExpression cronExpression
     */
    public void  addJob(String jobName, Class<? extends Job> jobClass, String cronExpression){
        String triggerName = jobClass.getName();
        addJob(jobName ,jobClass, triggerName , cronExpression );
    }

    public void addJob(String jobName, Class<? extends Job> jobClass, String triggerName,String cronExpression){
        addJob(jobName, jobClass, triggerName,  DEFAULT_JOB_GROUP,cronExpression );
    }

    public void addJob(String jobName, Class<? extends Job> jobClass, String triggerName, String group, String cronExpression){
        addJob(jobName, group,  triggerName, group, jobClass, cronExpression );
    }

    /**
     * 添加一个任务
     * @param jobName jobName
     * @param jobGroupName jobGroupName
     * @param triggerName triggerName
     * @param triggerGroupName triggerGroupName
     * @param jobClass jobClass
     * @param cronExpression cronExpression
     */
    private void addJob(String jobName, String jobGroupName,
                       String triggerName, String triggerGroupName, Class<? extends Job> jobClass,
                       String cronExpression) {
        log.info("add job，[jobName:{}],[jobGroupName:{}]," +
                "[triggerName:{}],[triggerGroupName:{}],[cronExpression:{}]",jobName, jobGroupName, triggerName, triggerGroupName,cronExpression);
        if ( !isValidExpression(cronExpression) ){
            log.info("cronExpression invalidate : {}", cronExpression);
            throw new RuntimeException("表达式不合法");
        }
        // 任务名，任务组，任务执行类
        JobDetail jobDetail = JobBuilder.newJob(jobClass)
                .withIdentity(jobName, jobGroupName)
                .storeDurably().build();
        // 触发器
        CronTrigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity(triggerName, triggerGroupName)
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                .build();
        TriggerKey triggerKey = new TriggerKey(triggerName, triggerGroupName);
        try {
            if (scheduler.checkExists(triggerKey)){
                scheduler.rescheduleJob(triggerKey, trigger);
            }else {
                scheduler.scheduleJob(jobDetail, trigger);
            }
        } catch (SchedulerException e) {
            log.error("新增任务异常：", e);
        }
    }

    /**
     * 暂停一个任务
     * <p>暂停任务然后恢复会根据你设置的频率执行 N 次,故在暂停时直接把触发器删了，然后恢复动作再重绑触发器</p>
     */
    public void pauseJob(String jobName , String triggerName){
        pauseJob(jobName, triggerName, DEFAULT_JOB_GROUP);
    }

    /**
     * 暂停一个任务
     * <p>暂停任务然后恢复会根据你设置的频率执行 N 次,故在暂停时直接把触发器删了，然后恢复动作再重绑触发器<p/>
     * @param jobName jobName
     * @param group group
     */
    public void pauseJob(String jobName , String triggerName, String group){
        try {
            log.info("pauseJob jobName:{},group: {}", jobName , group);
            scheduler.resumeTrigger(new TriggerKey(triggerName, group));
            scheduler.pauseJob(new JobKey(jobName,group));
        } catch (SchedulerException e) {
            log.error("暂停任务异常：", e);
        }
    }

    /**
     * 恢复一个任务
     * <p>暂停任务然后恢复会根据你设置的频率执行 N 次,故在暂停时直接把触发器删了，然后恢复动作再重绑触发器</p>
     * @param jobName jobName
     */
    public void resumeJob(String jobName , String triggerName , String cronExpression) {
        resumeJob(jobName, triggerName , DEFAULT_JOB_GROUP , cronExpression);
    }

    /**
     * 恢复一个任务
     * <p>暂停任务然后恢复会根据你设置的频率执行 N 次,故在暂停时直接把触发器删了，然后恢复动作再重绑触发器</p>
     * @param jobName jobName
     * @param group group
     */
    public void resumeJob(String jobName, String triggerName , String group , String cronExpression) {
        try {
            log.info("resumeJob jobName {}, triggerName:{},group: {}, cronExpression:{}", jobName , triggerName , group, cronExpression);
            if (!isValidExpression(cronExpression)){
                log.info("cronExpression invalidate : {}", cronExpression);
                throw new RuntimeException("表达式不合法");
            }
            // 触发器
            CronTrigger trigger = TriggerBuilder
                    .newTrigger()
                    .withIdentity(triggerName, group)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                    .build();
            scheduler.rescheduleJob( new TriggerKey(triggerName, group), trigger);
        } catch (SchedulerException e) {
            log.error("恢复任务异常：", e);
        }
    }

    /**
     * 移除一个任务
     * @param triggerName triggerName
     * @return
     */
    public boolean removeJob(String triggerName , String jobName) {
        return removeJob(triggerName, jobName , DEFAULT_GROUP);
    }


    public boolean removeJob(String triggerName, String jobName , String group) {
        log.info("removeJob triggerName is {},triggerGroupName is {}",triggerName, group);
        TriggerKey triggerKey = new TriggerKey(triggerName, group);
        JobKey jobKey = new JobKey(jobName , group);
        try {
            //停止触发器
            scheduler.pauseTrigger(triggerKey);
            //移除任务
            scheduler.deleteJob(jobKey);
            //移除触发器
            return scheduler.unscheduleJob(triggerKey);
        } catch (SchedulerException e) {
            log.error("移除任务异常：", e);
        }
        return false;
    }

    /**
     * 修改一个任务
     * @param triggerName triggerName
     * @param cronExpression cronExpression
     */
    public void modifyJob(String triggerName, String cronExpression){
        modifyJob(triggerName, DEFAULT_TRIGGER_GROUP_NAME, cronExpression);
    }

    /**
     * 修改一个任务
     * @param triggerName triggerName
     * @param triggerGroupName  triggerGroupName
     * @param cronExpression cronExpression
     */
    public void modifyJob(String triggerName, String triggerGroupName, String cronExpression){
        log.info("modifyJob triggerName is {},triggerGroupName is {}",triggerName, triggerGroupName);

        if ( !isValidExpression(cronExpression) ){
            log.info("cronExpression invalidate : {}", cronExpression);
            throw new RuntimeException("表达式不合法");
        }
        //通过触发器名和组名获取TriggerKey
        TriggerKey triggerKey = TriggerKey.triggerKey(triggerName, triggerGroupName);
        try {
            CronTrigger trigger = (CronTrigger) scheduler.getTrigger(triggerKey);
            if (null == trigger){
                log.info("CronTrigger is null");
                throw new RuntimeException("移除任务异常");
            }
            String oldTime = trigger.getCronExpression();
            if (!oldTime.equalsIgnoreCase(cronExpression)){
                trigger = TriggerBuilder.newTrigger()
                        .withIdentity(triggerKey)
                        .startNow()
                        .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                        .build();
                //按新的trigger重新设置job执行
                scheduler.rescheduleJob(triggerKey, trigger);
            }
        } catch (SchedulerException e) {
            log.error("修改任务异常：", e);
        }
    }

    private boolean isValidExpression(final String cron) {
        CronExpression cronExpression = null;
        boolean success = true;
        try {
            cronExpression = new CronExpression(cron);
        } catch (ParseException e) {
            success = false;
            log.info(e.getLocalizedMessage());
        }
        if ( !success ){
            return false;
        }
        CronTriggerImpl trigger = new CronTriggerImpl();
        trigger.setCronExpression(cronExpression);

        Date date = trigger.computeFirstFireTime(null);

        return date != null && date.after(new Date());
    }
}
