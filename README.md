package com.example;
import org.apache.flink.table.functions.ScalarFunction;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
public class ValidateDate extends ScalarFunction {
   // Define the date format
   private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
   // Method to validate the date
   public boolean eval(String date) {
       if (date == null) {
           return false;
       }
       try {
           // Validate the date string and if its invalid then an exception will be thrown
           LocalDate.parse(date, DATE_FORMATTER);
           return true;
       } catch (DateTimeParseException e) {
           return false;
       }
   }
}
