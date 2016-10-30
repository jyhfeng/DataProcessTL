package com.jyhfeng.queue.exception;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by dl85 on 2015/3/18.
 */
public class StringUtils {
    public static String getStringFromStackTrace(Throwable ex)
    {
        if (ex==null)
        {
            return "";
        }
        StringWriter str = new StringWriter();
        PrintWriter writer = new PrintWriter(str);
        try
        {
            ex.printStackTrace(writer);
            return str.getBuffer().toString();
        }
        finally
        {
            try
            {
                str.close();
                writer.close();
            }
            catch (IOException e)
            {
               e.printStackTrace();
            }
        }
    }
}
