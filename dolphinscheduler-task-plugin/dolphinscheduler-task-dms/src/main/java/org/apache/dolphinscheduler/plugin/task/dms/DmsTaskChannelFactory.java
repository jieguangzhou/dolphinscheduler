package org.apache.dolphinscheduler.plugin.task.dms;

import org.apache.dolphinscheduler.plugin.task.api.TaskChannel;
import org.apache.dolphinscheduler.plugin.task.api.TaskChannelFactory;
import org.apache.dolphinscheduler.spi.params.base.PluginParams;

import java.util.Collections;
import java.util.List;

public class DmsTaskChannelFactory implements TaskChannelFactory {

    @Override
    public TaskChannel create() {
        return new DmsTaskChannel();
    }

    @Override
    public String getName() {
        return "DMS";
    }

    @Override
    public List<PluginParams> getParams() {
        return Collections.emptyList();
    }
}
