package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Exercise for enriching TaxiRides with their corresponding TaxiFares using stateful processing.
 */
public class RidesAndFaresExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {
		// Extract parameters from the input arguments
		ParameterTool params = ParameterTool.fromArgs(args);
		String ridesInput = params.get("rides", pathToRideData);
		String faresInput = params.get("fares", pathToFareData);

		// Configuration parameters
		int maxDelay = 60;
		int speedFactor = 1800;

		// Setup the execution environment with checkpoints and web UI
		Configuration configuration = new Configuration();
		configuration.setString("state.backend", "filesystem");
		configuration.setString("state.savepoints.dir", "file:///tmp/savepoints");
		configuration.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(ExerciseBase.parallelism);

		env.enableCheckpointing(10000L);
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		DataStream<TaxiRide> rideStream = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesInput, maxDelay, speedFactor)))
				.filter(ride -> ride.isStart)
				.keyBy(ride -> ride.rideId);

		DataStream<TaxiFare> fareStream = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresInput, maxDelay, speedFactor)))
				.keyBy(fare -> fare.rideId);

			DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedStream = rideStream
				.connect(fareStream)
				.flatMap(new RideFareEnrichmentFunction())
				.uid("ride-fare-enrichment");

		printOrTest(enrichedStream);

		env.execute("Enrich Taxi Rides with Fares");
	}


	public static class RideFareEnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
		private ValueState<TaxiRide> rideState;
		private ValueState<TaxiFare> fareState;

		@Override
		public void open(Configuration parameters) {
			rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("ride-state", TaxiRide.class));
			fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("fare-state", TaxiFare.class));
		}

		@Override
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> collector) throws Exception {

			TaxiFare fare = fareState.value();

			if (fare == null) {
				rideState.update(ride);
				return;
			}

			fareState.clear();
			collector.collect(Tuple2.of(ride, fare));
		}

		@Override
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> collector) throws Exception {
			// Обработка TaxiFare.
			TaxiRide ride = rideState.value();

			if (ride == null) {
				fareState.update(fare);
				return;
			}

			rideState.clear();
			collector.collect(Tuple2.of(ride, fare));
		}


	}
}