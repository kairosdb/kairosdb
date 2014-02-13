package org.kairosdb.core.aggregator;

import com.goebl.simplify.PointExtractor;
import org.kairosdb.core.DataPoint;

/**
* Created by jrussek on 11.02.14.
*/
class SimplifyPointExtrator implements PointExtractor<DataPoint> {
    @Override
    public double getX(DataPoint dataPoint) {
        return dataPoint.getTimestamp();
    }

    @Override
    public double getY(DataPoint dataPoint) {
        return dataPoint.getDoubleValue();
    }
}
