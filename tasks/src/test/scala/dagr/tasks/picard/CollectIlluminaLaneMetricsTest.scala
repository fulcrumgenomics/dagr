/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

package dagr.tasks.picard

import dagr.commons.util.UnitSpec

class CollectIlluminaLaneMetricsTest extends UnitSpec {
  "CollectIlluminaLaneMetrics.convertBarcodeAndSkipToTemplate" should "convert barcodes and skips to template" in {
    CollectIlluminaLaneMetrics.convertBarcodeAndSkipToTemplate("150T") shouldBe "150T"
    CollectIlluminaLaneMetrics.convertBarcodeAndSkipToTemplate("8S150T8S") shouldBe "166T"
    CollectIlluminaLaneMetrics.convertBarcodeAndSkipToTemplate("8S150T8S8M") shouldBe "174T"
    CollectIlluminaLaneMetrics.convertBarcodeAndSkipToTemplate("8M150T8B8B") shouldBe "158T16B"
    CollectIlluminaLaneMetrics.convertBarcodeAndSkipToTemplate("8M1S142T8B8B8M1S142T") shouldBe "151T16B151T"
  }
}
