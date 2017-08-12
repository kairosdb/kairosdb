===============================
Adding Aggregators and GroupBys
===============================
KariosDB ships with a predefined set of aggregators and groupBys. If these don't meet your purpose it is easy to add new ones.
These are added as plugins. See the :doc:`Plugins documentation <Plugins>` for information about creating plugins.

--------------------
Add a new Aggregator
--------------------
Aggregators are added as a plugin:
  #. Create a class that implements the Aggregator interface. If your aggregator needs to evaluate over a range of time, then your aggregators should extend RangeAggregator.
  #. Implement the logic for the aggregator.
  #. Add `Annotations`_.
  #. Create a properties file named kairosdb-<aggregatorName>.properties that contains

     #. kairosdb.service.<aggregator>=org.kairosdb.plugin.aggs.<AggregatorName>Module where <AggregatorName>Module is a Guice module which binds your new aggregator.

-----------------
Add a new GroupBy
-----------------
GroupBys are also added as a plugin:
  #. Create a class that implements the GroupBy interface.
  #. Implement the logic for the groupBy.
  #. Add `Annotations`_.
  #. Create a properties file named kairosdb-<groupByName>.properties that contains

     #. kairosdb.service.<groupBy>=org.kairosdb.plugin.aggs.<GroupByName>Module where <GroupByName>Module is a Guice module which binds your new groupBy.

-----------
Annotations
-----------
GroupBys and aggregators require annotations. They require a name annotation (AggregatorName and GroupByName)
which is applied to the class and QueryPropery or QueryCompoundProperty annotations which are applied to member
variables (properties of the aggregator or groupBy).

The :doc:`groupbys </restapi/ListGroupBys>` and :doc:`aggregators </restapi/ListAggregators>` REST APIs return a list of groupBys and aggregators and their properties.
These APIs are used by clients such as the UI to help facilitate the creation of queries.


##############
AggregatorName
##############
This annotation is applied to the Aggregator class. It defines the name of the aggregator and its description.

**name**

The name of the aggregator (required).

**description**

A description of the aggregator (required).

###########
GroupByName
###########
This annotation is applied to the GroupBy class. It defines the name of the groupBy and its description.

**name**

The name of the groupBy (required).

**description**

A description of the groupBy (required).

#############
QueryProperty
#############
The QueryProperty annotation defines the properties of a groupBy or aggregator. It is applied to member variables that will be exposed as properties.

**name**

The name of the property (optional). By default this is the name of the member variable.

**label**

The display name of the property (required). This is the value used by the UI.

**description**

A description of the property (required).

**optional**

Whether the property is optional or not (optional). Defaults to false.

**type**

The data type of the property (optional). By default, the type is calculated by using reflection on the member variable.

**options**

The list of values if the type is "enum" (optional).

**default_value**

The default value for the property (optional). If not specified, a default value is calculated.

**validation**

A boolean expression used to test if the value is valid (optional). The expression is written in Javascript.

#####################
QueryCompoundProperty
#####################
The QueryCompoundProperty annotation defines the properties of a groupBy or aggregator for a member variable that contains multiple variables.

**name**

The name of the property (optional). By default this is the name of the member variable.

**label**

The display name of the property (required). This is the value used by the UI.

**order**

This defines the order of properties (optional). This is a list of labels of the properties.