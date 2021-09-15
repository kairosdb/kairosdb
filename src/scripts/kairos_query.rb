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

$query = {}
$query[:metrics] = []
$query[:metrics][0] = {}

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
		if @return_code == 0
			@return_code = 1
		end
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

$monitor = Monitor.new

def parse_time(param)
	_, number, unit = /(\d+)(\w+)/.match(param).to_a.flatten

	# if result is nill then fail with error
	# p number
	# p unit

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

	ret = {}
	ret[:value] = number
	ret[:unit] = unit

	ret
end

def print_results(name, groups, values)
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

def check_results(_name, groups, values)
	group_str_arr = []

	groups.each do |key, value|
		group_str_arr.append("#{key}=#{value}")
	end

	values.each do |value|
		if !$monitor.crit_min.nil? && value[1].to_f < $monitor.crit_min
			$monitor.add_crit_message("[#{group_str_arr.join(',')}] #{value[1]} is less than mincrit of #{$monitor.crit_min}")
			break
		end

		if !$monitor.crit_max.nil? &&  value[1].to_f > $monitor.crit_max
			$monitor.add_crit_message("[#{group_str_arr.join(',')}] #{value[1]} is greater than maxcrit of #{$monitor.crit_max}")
			break
		end

		if !$monitor.warn_min.nil? && value[1].to_f < $monitor.warn_min
			$monitor.add_warn_message("[#{group_str_arr.join(',')}] #{value[1]} is less than minwarn of #{$monitor.warn_min}")
			break
		end

		if !$monitor.warn_max.nil? && value[1].to_f > $monitor.warn_max
			$monitor.add_warn_message("[#{group_str_arr.join(',')}] #{value[1]} is greater than maxwarn of #{$monitor.warn_max}")
			break
		end

	end
end

OptionParser.new do |opts|
	opts.banner = 'Usage: bla bla bla'

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

	opts.on('-e', '--end <end time>', 'Start of query') do |v|
		end_time = $query[:end_relative] = {}
		number, unit = parse_time v
		end_time[:value] = number
		end_time[:unit] = unit
	end

	opts.on('-t', '--tags <key=value,key2=value,...>', 'Start of query') do |v|
		tags = $metric_query[:tags] = {}

		tag_arr = v.split(',')

		tag_arr.each do |tag_pair|
			_, key, value = /(\w+)=(\w+)/.match(tag_pair).to_a.flatten

			tags[key] = [] if tags[key].nil?

			tags[key].append(value)
		end
	end

	opts.on('-a', '--aggregate <aggregator string>', 'Start of query') do |v|
		aggregators = v.split('|')
		# puts aggregators
		query_aggs = $metric_query[:aggregators] = []

		aggregators.each do |agg|
			_, agg_name, argument = /(\w+)\((.*)\)/.match(agg).to_a.flatten

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

	opts.on('-g', '--group-by <group1,group2,...>', 'Start of query') do |v|
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

	opts.on('--on_empty ok|warn|crit',
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

	opts.on('--on_timeout <value>', 'Maximum critical threshold to monitor') do |v|
		$monitor.crit_max = v
	end
end.parse!

# puts query.to_json
uri = URI.parse("#{$host}/api/v1/datapoints/query")
http = Net::HTTP.new(uri.host, uri.port)
request = Net::HTTP::Post.new(uri.path, 'Content-Type' => 'application/json')
request.body = $query.to_json

response = http.request(request)

if response.code.to_i != 200
	abort("Query failed: #{response.body}")
end

json_resp = JSON.parse(response.body)


# We only query a single metric so there is only one query result
query_results = json_resp['queries'][0]['results']

# puts query_results[0]
unless query_results.nil?
	query_results.each do |result|

		name = result['name']
		groups = {}

		result['group_by'].each do |group_by|
			groups = group_by['group'] if group_by['name'] == 'tag'
		end

		if $monitor.monitor_set?
			check_results(name, groups, result['values'])
		else
			print_results(name, groups, result['values'])
		end
	end
end



if $monitor.monitor_set?
	puts $monitor.get_message
	exit($monitor.return_code)
end