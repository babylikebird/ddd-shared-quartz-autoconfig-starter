package org.ddd.shared.quartz.autoconfig.starter;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.quartz.QuartzAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

/**
 * @author Mr.Yangxiufeng
 * @date 2020-10-13
 * @time 14:23
 */
@Configuration
@AutoConfigureAfter({ DataSourceAutoConfiguration.class, HibernateJpaAutoConfiguration.class,
        LiquibaseAutoConfiguration.class, FlywayAutoConfiguration.class,  QuartzAutoConfiguration.class})
public class QuartzCustomAutoConfiguration {

    @Bean
    public QuartzSchedulerManager quartzSchedulerManager(SchedulerFactoryBean schedulerFactoryBean){
        return new QuartzSchedulerManager(schedulerFactoryBean.getScheduler());
    }

}
