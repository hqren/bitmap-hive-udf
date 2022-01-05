package com.data.tools.udfa;

import com.data.tools.util.RoaringBitmapUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

/**
 * hive
 * add jar /opt/jobs/bitmap-hive-udf.jar;
 * create temporary function rb_xor_agg as 'com.data.tools.udfa.XOrAggUDFA';
 */
public class XOrAggUDFA extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected...");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " is passed...");
        }

        return new BitmapGenericUDAFEvaluator();
    }

    public static class BitmapGenericUDAFEvaluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputOI;
        private Text result;

        static class SumLongAgg extends AbstractAggregationBuffer {
            boolean empty;
            String sum;

            @Override
            public int estimate() {
                return JavaDataModel.PRIMITIVES1 + JavaDataModel.PRIMITIVES2;
            }
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);
            result = new Text();
            inputOI = (PrimitiveObjectInspector) parameters[0];

//            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumLongAgg result = new SumLongAgg();
            reset(result);

            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SumLongAgg myagg = (SumLongAgg) agg;
            myagg.empty = true;
            myagg.sum = null;
        }

        private boolean warned = false;

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            try {
                merge(agg, parameters[0]);
            } catch (NumberFormatException e) {
                if (!warned) {
                    warned = true;
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                SumLongAgg myagg = (SumLongAgg) agg;

                String oldValue = myagg.sum;
                String newValue = PrimitiveObjectInspectorUtils.getString(partial, inputOI);

                myagg.sum = RoaringBitmapUtils.xOr(oldValue, newValue);
                myagg.empty = false;
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SumLongAgg myagg = (SumLongAgg) agg;
            if (myagg.empty) {
                return null;
            }

            result.set(myagg.sum);

            return result;
        }
    }
}
