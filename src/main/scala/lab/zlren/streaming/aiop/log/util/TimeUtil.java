package lab.zlren.streaming.aiop.log.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zlren
 * @date 2018-01-19
 */
public class TimeUtil {

    /**
     * 日期字符串转时间
     *
     * @param timeStr
     * @return
     * @throws ParseException
     */
    public Date strToDate(String timeStr) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.parse(timeStr);
    }
}
