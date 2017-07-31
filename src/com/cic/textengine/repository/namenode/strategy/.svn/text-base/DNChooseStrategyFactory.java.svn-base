package com.cic.textengine.repository.namenode.strategy;

import com.cic.textengine.repository.namenode.strategy.impl.DNChooseStrategyImpl;

public class DNChooseStrategyFactory {
	static DNChooseStrategy m_instance = null;
	
	public static synchronized DNChooseStrategy getDNChooseStrategyInstance(){
		if (m_instance == null){
			m_instance = new DNChooseStrategyImpl();
		}
		return m_instance;
	}
}
