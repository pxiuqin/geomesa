/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

//标识索引不重复出现的基数
object Cardinality  extends Enumeration {
  type Cardinality = Value
  val HIGH    = Value("high")
  val LOW     = Value("low")
  val UNKNOWN = Value("unknown")
}
