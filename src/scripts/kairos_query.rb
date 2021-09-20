#!/usr/bin/ruby
# frozen_string_literal: true

# kairos_query.rb metric.name --from 1h --to 1m --group-by "topic datacenter"
# --tags "topic=AttachmentFeed,format=binary" --aggregate
# "sum(1m)|trim(both)|avg(10m)"

# sum, avg, min, max, count, trim,

# sum(1m)|avg(10m)|trim(both)

# trim(BOTH)

require 'optparse'
require 'json'
require 'uri'
require 'net/http'
require 'date'

$query = { metrics: [{}]}

$host = 'http://localhost:8080'

$metric_query = $query[:metrics][0]

$metric_name = nil

class Monitor
	attr_accessor :warn_min, :warn_max, :crit_min, :crit_max, :on_empty,
								:on_timeout

	attr_reader :return_code

	def initialize
		@warn_min = nil
		@warn_max = nil
		@crit_max = nil
		@crit_min = nil
		@on_empty = :warn
		@on_timeout = nil
		@crit_messages = []
		@warn_messages = []
		@return_code = 0
	end

	def monitor_set?
		!@warn_min.nil? || !@warn_max.nil? || !@crit_max.nil? || !@crit_min.nil?
	end

	def add_crit_message(msg)
		@crit_messages.append(msg)
		@return_code = 2
	end

	def add_warn_message(msg)
		@warn_messages.append(msg)
		@return_code = 1 if @return_code == 0
	end

	def get_message
		case @return_code
		when 0
			"OK: \n"
		when 1
			"WARNING: #{$metric_name} #{@warn_messages.join(';')}\n"
		when 2
			"CRITICAL: #{$metric_name} #{@crit_messages.join(';')}\n"
		end
	end

end

class QueryResults

	def initialize(query_results)
		@query_results = query_results
	end

	def empty?
		@query_results.nil?
	end

	def each
		@query_results.each do |result|

			name = result['name']

			groups = {}

			if result['group_by']
				result['group_by'].each do |group_by|
					groups = group_by['group'] if group_by['name'] == 'tag'
				end
			end

			yield(name, groups, result['values'])
		end
	end
end

$monitor = Monitor.new

def parse_time(param)
	_, number, unit = /(\d+)(\w+)/.match(param).to_a

	case unit
	when 'ms'
		unit = 'milliseconds'
	when 's'
		unit = 'seconds'
	when 'm'
		unit = 'minutes'
	when 'h'
		unit = 'hours'
	when 'd'
		unit = 'days'
	when 'w'
		unit = 'weeks'
	when 'y'
		unit = 'years'
	end

	[number, unit]
end

def get_sampling(sampling)
	number, unit = parse_time(sampling)

	{value: value, unit: unit}
end

def print_results(query_results)
	unless query_results.nil?

		query_results.each do |name, groups, values|
			group_str_arr = []

			groups.each do |key, value|
				group_str_arr.append("#{key}=#{value}")
			end

			values.each do |value|
				str = "#{Time.at(value[0] / 1000).to_datetime.strftime('%FT%T.%L%:z')} "
				str += "#{group_str_arr.join(',')} #{name}=#{value[1].to_s}"
				puts str
			end
		end
	end
end

def check_results(query_results)

	if !query_results.nil?
		query_results.each do |name, groups, values|
			group_str_arr = []

			groups.each do |key, value|
				group_str_arr.append("#{key}=#{value}")
			end

			if values.empty?
				case $monitor.on_empty
				when :warn
					$monitor.add_warn_message('Empty results returned from query.')
				when :crit
					$monitor.add_crit_message('Empty results returned from query.')
				end
			else
				warn_message = nil
				values.each do |value|
					if !$monitor.crit_min.nil? && value[1].to_f < $monitor.crit_min
						$monitor.add_crit_message("[#{group_str_arr.join(',')}] #{value[1]} is less than mincrit of #{$monitor.crit_min}")
						break
					end

					if !$monitor.crit_max.nil? &&  value[1].to_f > $monitor.crit_max
						$monitor.add_crit_message("[#{group_str_arr.join(',')}] #{value[1]} is greater than maxcrit of #{$monitor.crit_max}")
						break
					end

					# If we get a critical then we bail out.  We don't bail out on warnings
					# in case a critical is yet to come.

					if !$monitor.warn_min.nil? && value[1].to_f < $monitor.warn_min
						warn_message = "[#{group_str_arr.join(',')}] #{value[1]} is less than minwarn of #{$monitor.warn_min}"
					end

					if !$monitor.warn_max.nil? && value[1].to_f > $monitor.warn_max
						warn_message = "[#{group_str_arr.join(',')}] #{value[1]} is greater than maxwarn of #{$monitor.warn_max}"
					end

				end

				$monitor.add_warn_message(warn_message) unless warn_message.nil?
			end
		end
	else
		case $monitor.on_empty
		when :warn
			$monitor.add_warn_message('Empty results returned from query.')
		when :crit
			$monitor.add_crit_message('Empty results returned from query.')
		end
	end
end

OptionParser.new do |opts|
	opts.banner =
		'KairosDB query/monitor script.  This script lets you both query Kairos for data
as well as monitor data.  The monitoring was designed to be used with Nagios NRPE checks.
Usage: kairos_query.rb -h <kairos_host> -m <metric> -s <start_time> -e <end_time>
    -t <filter_tags> -a <aggregators> -g <groups>

For time options such as start and end time the format is a number followed by a time unit,
ex. 10m (10 minutes) or 30s (30 seconds).
Allowed time units are ms, s, m, h, d, w, y.

Supported aggregators are sum, avg, min, max, count and trim.  For sum, avg,
min, max and count you have to specify a sampling period, for example sum(10m) - this
will sum over a 10 minute window.

If you specify one or more of the monitor options (minwarn, maxwarn, mincrit or maxcrit)
the script runs in monitor mode.  Monitor mode does not return data but returns codes
according to monitor options specified.
0 - if all values are withing specified range.
1 - if any value is in the warn range.
2 - if any value is in the critical range.

Monitor mode was designed to be used as an NRPE check with nagios.

Available options:'

	opts.on('-h', '--host <kairos url>', 'Ulr to Kairos host') do |v|
		$host = v
	end

	opts.on('-m', '--metric <metric>', 'Metric name') do |v|
		$metric_query[:name] = v
		$metric_name = v
	end

	opts.on('-s', '--start <start time>', 'Start of query') do |v|
		start = $query[:start_relative] = {}
		number, unit = parse_time v
		start[:value] = number
		start[:unit] = unit
	end

	opts.on('-e', '--end <end time>', 'End of query') do |v|
		end_time = $query[:end_relative] = {}
		number, unit = parse_time v
		end_time[:value] = number
		end_time[:unit] = unit
	end

	opts.on('-t', '--tags <key=value,key2=value,...>', 'Tags to filter on') do |v|
		tags = $metric_query[:tags] = {}

		tag_arr = v.split(',')

		tag_arr.each do |tag_pair|
			_, key, value = /(.+)=(.+)/.match(tag_pair).to_a

			tags[key] = [] if tags[key].nil?

			tags[key].append(value)
		end
	end

	opts.on('-a', '--aggregate <aggregator string>', 'Aggregators for data.  Multiple aggregators are piped together "sum(1m)|avg(10m)"') do |v|
		aggregators = v.split('|')
		# puts aggregators
		query_aggs = $metric_query[:aggregators] = []

		aggregators.each do |agg|
			_, agg_name, argument = /(\w+)\((.*)\)/.match(agg).to_a

			agg_json = {}
			agg_json[:name] = agg_name

			case agg_name
			when 'sum', 'avg', 'min', 'max', 'count'
				agg_json[:sampling] = get_sampling(argument)
			when 'trim'
				agg_json[:trim] = argument
			else
				abort("Unrecognized aggregator: #{agg_name}")
			end

			query_aggs.append(agg_json)
		end
	end

	opts.on('-g', '--group-by <group1,group2,...>', 'Tags to group on') do |v|
		groups = v.split(',')
		$metric_query[:group_by] = []
		group_tags = {}
		$metric_query[:group_by].append(group_tags)
		group_tags[:name] = 'tag'
		group_tags[:tags] = groups
	end

	# Monitor options
	opts.on('--minwarn <value>', 'Minimum warning threshold to monitor') do |v|
		$monitor.warn_min = v.to_f
		if !$monitor.warn_max.nil? && $monitor.warn_min > $monitor.warn_max
			abort("minwarn must be less than maxwarn")
		end
	end

	opts.on('--maxwarn <value>', 'Maximum warning threshold to monitor') do |v|
		$monitor.warn_max = v.to_f
		if !$monitor.warn_min.nil? && $monitor.warn_min > $monitor.warn_max
			abort("minwarn must be less than maxwarn")
		end
	end

	opts.on('--mincrit <value>', 'Minimum critical threshold to monitor') do |v|
		$monitor.crit_min = v.to_f
		if !$monitor.crit_max.nil? && $monitor.crit_min > $monitor.crit_max
			abort("mincrit must be less than maxcrit")
		end
	end

	opts.on('--maxcrit <value>', 'Maximum critical threshold to monitor') do |v|
		$monitor.crit_max = v.to_f
		if !$monitor.crit_min.nil? && $monitor.crit_min > $monitor.crit_max
			abort("mincrit must be less than maxcrit")
		end
	end

	opts.on('--on-empty ok|warn|crit',
					'If no metrics are returned should it be "ok" "warn" or "crit" (default warn)') do |v|
		case v
		when 'ok'
			$monitor.on_empty = :ok
		when 'warn'
			$monitor.on_empty = :warn
		when 'crit'
			$monitor.on_empty = :crit
		else
			abort('Unrecognized on_empty option, must be ok, warn or crit')
		end
	end

	opts.on('--on-timeout <value>', 'Maximum critical threshold to monitor') do |v|
		$monitor.on_timeout = v
	end
end.parse!

#puts $query.to_json
uri = URI.parse("#{$host}/api/v1/datapoints/query")
http = Net::HTTP.new(uri.host, uri.port)
request = Net::HTTP::Post.new(uri, 'Content-Type' => 'application/json')
request.body = $query.to_json

response = http.request(request)

if response.code.to_i != 200
	abort("Query failed: #{response.body}")
end

json_resp = JSON.parse(response.body)


# We only query a single metric so there is only one query result
query_results = QueryResults.new(json_resp['queries'][0]['results'])

#puts json_resp['queries'][0]['results']

if $monitor.monitor_set?
	check_results(query_results)
	puts $monitor.get_message
	exit($monitor.return_code)
else
	print_results(query_results)
end


