CREATE OR REPLACE FUNCTION kairosdb.tag_filter(row_tags map<text, text>, filter_tags map<text, text>)
  RETURNS NULL ON NULL INPUT 
  RETURNS BOOLEAN
  LANGUAGE java AS $$
    for (Map.Entry<String, String> filterEntry : filter_tags.entrySet())
    {
      String value = row_tags.get(filterEntry.getKey());
      if (value == null || !filterEntry.getValue().equals(value))
        return false;
    }
    return true;

	$$;