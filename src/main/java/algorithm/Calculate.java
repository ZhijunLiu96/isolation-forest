package algorithm;

import java.util.List;

public class Calculate {

    /**
     * To calculate C-factor
     * @param: Integer - n
     * @return: Double - C-factor
     */
    public static double calculate_Cvalue(int n) {
        double res = 2 * calculate_Hvalue(n - 1) - (((double) 2 * (n - 1)) / n);
        return res;
    }

    /**
     * To calculate Harmonic Number
     * @param: Integer - i
     * @return: Double - Harmonic Number
     */
    private static double calculate_Hvalue(int i) {
        return Math.log(i) + 0.5772156649;
    }

    /**
     * To calculate Anomaly Score
     * @param: Double - Average Path-length, Integer n
     * @param: Integer n
     * @return Double - Anomaly Score
     */
    public static double calculate_anomalyScore(double pathLengthAvg, int n) {
        double res = -1 * (pathLengthAvg / calculate_Cvalue(n));
        return Math.pow(2, res);
    }

    public static double calculateAverage(List<? extends Number> values) {
        if (values == null || values.isEmpty()) {
            return 0;
        }
        double avg = 0;
        avg = calculateSum(values)/values.size();
        return avg;
    }

    public static double calculateSum(List<? extends Number> values) {
        if (values == null || values.isEmpty()) {
            return 0;
        }
        double sum = 0;
        for (Number number:values) {
            sum += number.doubleValue();
        }
        return sum;
    }


}
