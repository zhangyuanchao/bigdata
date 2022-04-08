package com.themis.flink.window;

import com.themis.flink.model.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {

    private boolean running = true; // 控制数据的生成
    @Override
    public void run(SourceContext<Event> context) throws Exception {
        Random random = new Random();
        String[] users = {"Alice", "Bob", "Lily", "Mary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=100", "./prod?id=200"};
        while(running) {
            context.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
