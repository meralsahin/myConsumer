package com.meral.kinesisconsumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class RecordProducerFactory implements IRecordProcessorFactory {

	@Override
	public IRecordProcessor createProcessor() {
		return new RecordProcessor();
	}

}
