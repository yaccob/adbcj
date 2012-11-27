package org.adbcj.support;

import org.adbcj.Field;
import org.adbcj.ResultHandler;
import org.adbcj.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* @author roman.stoffel@gamlor.info
* @since 10.05.12
*/
public class DefaultResultEventsHandler implements ResultHandler<DefaultResultSet> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultResultEventsHandler.class);

    private Value[] currentRow;
    private int rowIndex;

    public void startFields(DefaultResultSet accumulator) {
        logger.trace("ResultSetEventHandler: startFields");
    }

    public void field(Field field, DefaultResultSet accumulator) {
        logger.trace("ResultSetEventHandler: field");
        accumulator.addField(field);
    }

    public void endFields(DefaultResultSet accumulator) {
        logger.trace("ResultSetEventHandler: endFields");
    }

    public void startResults(DefaultResultSet accumulator) {
        logger.trace("ResultSetEventHandler: startResults");
    }

    public void startRow(DefaultResultSet accumulator) {
        logger.trace("ResultSetEventHandler: startRow");

        int columnCount = accumulator.getFields().size();
        currentRow = new Value[columnCount];
    }

    public void value(Value value, DefaultResultSet accumulator) {
        logger.trace("ResultSetEventHandler: value");

        currentRow[rowIndex%currentRow.length] = value;
        rowIndex++;
    }

    public void endRow(DefaultResultSet accumulator) {
        logger.trace("ResultSetEventHandler: endRow");
        DefaultRow row = new DefaultRow(accumulator, currentRow);
        accumulator.addResult(row);
        currentRow = null;
    }

    public void endResults(DefaultResultSet accumulator) {
        logger.trace("ResultSetEventHandler: endResults");
    }

    public void exception(Throwable t, DefaultResultSet accumulator) {
    }
}
