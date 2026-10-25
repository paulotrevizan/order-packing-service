package pt.paulotrevizan.orderpacking.common.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

/** The reconciler should be off in the demo publisher, which only produces records. */
@Configuration(proxyBeanMethods = false)
@Profile("!demo")
@EnableScheduling
class SchedulingConfiguration {}
