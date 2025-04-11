#!/bin/bash

# 该脚本的作用是初始化所有的维度表到Hbase，只需执行一次，执行前需启动spark程序

MAXWELL_HOME=/opt/module/maxwell

import_data() {
    for tab in $@
    do
      $MAXWELL_HOME/bin/maxwell-bootstrap --database marketing --table ${tab} --config $MAXWELL_HOME/config.properties
    done
}
$MAXWELL_HOME/bin/maxwell-bootstrap --database marketing_config --table table_process_dim --config $MAXWELL_HOME/config.properties
import_data customer_info customer_label product_info real_time_event_definition

