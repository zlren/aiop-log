package lab.zlren.streaming.log;

import lombok.extern.slf4j.Slf4j;

/**
 * 模拟日志产生
 *
 * @author zlren
 * @date 2017-12-12
 */
@Slf4j
public class LoggerGenerator {

    public static void main(String[] args) throws InterruptedException {
        int index = 0;
        while (true) {
            Thread.sleep(1000);
            log.info("cur value is {}", index++);
        }
    }
}
