package com.nick.hiveutils.udf;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class AggregateMD5 extends AbstractGenericUDAFResolver {

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] types) {
		return new AggregateMD5Evaluator();
	}

	static class AggregateMD5Agg implements AggregationBuffer {
		ArrayList<BytesWritable> data;
		MessageDigest md5;
	};

	public static class AggregateMD5Evaluator extends GenericUDAFEvaluator {

		private ArrayList<PrimitiveObjectInspector> ois = new ArrayList<PrimitiveObjectInspector>();
		private StandardListObjectInspector partialOI;

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {

			super.init(m, parameters);

			// initialize input inspectors
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				for (ObjectInspector oi : parameters) {
					if (oi instanceof PrimitiveObjectInspector) {
						ois.add((PrimitiveObjectInspector) oi);
					}

				}
			}

			else {
				partialOI = (StandardListObjectInspector) parameters[0];
			}

			// init output object inspectors
			if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
				// The output of a partial aggregation is a list of binaries representing the
				// data from the rows
				return ObjectInspectorFactory
						.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
			} else {

				return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
			}
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			AggregateMD5Agg agg = new AggregateMD5Agg();
			reset(agg);
			return agg;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			AggregateMD5Agg md5Agg = (AggregateMD5Agg) agg;
			try {
				md5Agg.md5 = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
				throw new HiveException(e);
			}
			md5Agg.data = new ArrayList<BytesWritable>();
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			int objectCounter = 0;

			AggregateMD5Agg md5Agg = (AggregateMD5Agg) agg;
			for (Object obj : parameters) {
				Object fieldValue = ois.get(objectCounter).getPrimitiveJavaObject(obj);
				byte[] bytes = fieldValue.toString().getBytes();
				md5Agg.md5.update(bytes);
				md5Agg.data.add(new BytesWritable(bytes));
				objectCounter++;
			}
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			AggregateMD5Agg md5Agg = (AggregateMD5Agg) agg;
			return md5Agg.data;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			List<BytesWritable> data = (List<BytesWritable>) partialOI.getList(partial);
			AggregateMD5Agg md5Agg = (AggregateMD5Agg) agg;
			for (BytesWritable bytes : data) {
				md5Agg.md5.update(bytes.copyBytes());
				md5Agg.data.add(bytes);
			}
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			AggregateMD5Agg md5Agg = (AggregateMD5Agg) agg;
			byte[] md5bytes = md5Agg.md5.digest();
			return new Text(String.format("%040x", new BigInteger(1, md5bytes)));
		}
	}
}
