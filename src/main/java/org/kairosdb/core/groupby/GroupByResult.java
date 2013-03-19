//
//  GroupByResult.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.core.groupby;

import org.kairosdb.core.formatter.FormatterException;

public interface GroupByResult
{
	String toJson() throws FormatterException;
}