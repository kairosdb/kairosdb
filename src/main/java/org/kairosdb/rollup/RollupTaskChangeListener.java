package org.kairosdb.rollup;

public interface RollupTaskChangeListener
{
	enum Action
	{
		ADDED,
		CHANGED,
		REMOVED
	}

	void change(RollupTask task, Action action);

}
