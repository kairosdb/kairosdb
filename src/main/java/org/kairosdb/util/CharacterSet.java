/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CharacterSet
{
  private static final Logger log = LoggerFactory.getLogger(CharacterSet.class);

  private static final String CHARACTER_SET_REGEX_PATTERN = "kairosdb.character.set.regex.pattern";
  private static CharacterSet internalCharacterSet;
  private Pattern regex;

  private CharacterSet()
  {
  }

  /**
   * Static wrapper for internal version of isValid
   */
  public static boolean isValid(String s)
  {
    return provideInternalCharacterSet().isValidInternal(s);
  }

  /**
   * Returns true if the specified string contains a valid set of characters
   * @param s string to test
   * @return true if all characters in the string are valid
   */
  private boolean isValidInternal(String s)
  {
    Matcher matcher = regex.matcher(s);
    return matcher.matches();
  }

  /**
   * Provides a static CharacterSet, which is used by isValid.
   * Creates a new CharacterSet object only on first pass.
   * @return the static CharacterSet
   */
  private static CharacterSet provideInternalCharacterSet() {
    if (internalCharacterSet == null) {
      internalCharacterSet = newCharacterSet();
    }
    return internalCharacterSet;
  }

  /**
   * Factory method for creating a CharacterSet object with a configurable pattern
   */
  private static CharacterSet newCharacterSet() {
    CharacterSet cs = new CharacterSet();
    try {
      cs.setPatternFromConfig();
    } catch (IOException ex) {
      // ignore for now
    }
    return cs;
  }

  /**
   * Read the regex pattern for validation from kairosdb.properties file and set
   */
  private void setPatternFromConfig() throws IOException {
    Properties props = new Properties();
    InputStream is = getClass().getClassLoader().getResourceAsStream("kairosdb.properties");
    props.load(is);
    is.close();

    String regexPattern = props.getProperty(CHARACTER_SET_REGEX_PATTERN);
    log.info("CharacterSet validation regex pattern: {}", regexPattern);
    regex = Pattern.compile(regexPattern);
  }
}