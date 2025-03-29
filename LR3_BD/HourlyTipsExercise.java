
package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// Чтение параметров
		ParameterTool params = ParameterTool.fromArgs(args);
		final String inputPath = params.get("input", ExerciseBase.pathToFareData);

		final int maxDelay = 60;
		final int speedFactor = 600;

		// Настройка среды выполнения Flink
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// Источник данных с чаевыми
		DataStream<TaxiFare> fareStream = env.addSource(
				fareSourceOrTest(new TaxiFareSource(inputPath, maxDelay, speedFactor)));

		DataStream<Tuple3<Long, Long, Float>> hourlyMaxTips = fareStream
				.keyBy(fare -> fare.driverId)
				.timeWindow(Time.hours(1))
				.aggregate(new SumTipsAggregate(), new MaxTipsWindowFunction())
				.timeWindowAll(Time.hours(1))
				.maxBy(2);

		printOrTest(hourlyMaxTips);

		// Запуск выполнения пайплайна
		env.execute("Hourly Tips (java)");
	}
	public static class SumTipsAggregate implements AggregateFunction<TaxiFare, Float, Float> {

		@Override
		public Float createAccumulator() {
			return 0.0f;
		}

		@Override
		public Float add(TaxiFare value, Float accumulator) {
			return accumulator + value.tip;
		}

		@Override
		public Float getResult(Float accumulator) {
			return accumulator;
		}

		@Override
		public Float merge(Float a, Float b) {
			return a + b;
		}
	}

	public static class MaxTipsWindowFunction extends ProcessWindowFunction<
			Float, Tuple3<Long, Long, Float>, Long, TimeWindow> {

		@Override
		public void process(Long key, Context context, Iterable<Float> input, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			Float totalTips = input.iterator().next();
			out.collect(new Tuple3<>(context.window().getEnd(), key, totalTips));
		}
	}

}


