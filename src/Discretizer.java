import java.util.*;

/**
 * 
 * <p>
 * Title: Opportunity Map
 * </p>
 * 
 * <p>
 * Description: Discretizer
 * </p>

 */

public class Discretizer
{

	private Hashtable classSamplingRatio_ = null;

	private AbstractPreprocessColumnDataReader preClassDataReader_;
	private HDFSLineFileReader preClassDataReaderOnHDFS_;

	private AbstractPreprocessColumnDataReader preQuantityDataReader_;
	private HDFSLineFileReader preQuantityDataReaderOnHDFS_;

	/* project target data folder */
	private String projectTargetFolder_ = null;

	/* &&& this two variables are only for entropy discretization */
	private String classAttributeFileName_ = null;
	private String quantityAttributeFileName_ = null;

	private EntropyDiscretizer entropyDiscretizer_ = null;
	private EntropyDiscretizerOnHadoop entropyDiscretizerOnHadoop_ = null;
	
	public Discretizer(AbstractPreprocessColumnDataReader classFileReader, Hashtable classSamplinghash, AbstractPreprocessColumnDataReader quantityFileReader,
			String saveDataFolder)
	{

		preClassDataReader_ = classFileReader;
		classAttributeFileName_ = classFileReader.getFileName();

		preQuantityDataReader_ = quantityFileReader;
		if (quantityFileReader != null)
		{
			quantityAttributeFileName_ = quantityFileReader.getFileName();
		}

		classSamplingRatio_ = classSamplinghash;
		projectTargetFolder_ = saveDataFolder;

		// initialize entropy discretizer
		entropyDiscretizer_ = new EntropyDiscretizer(classAttributeFileName_, quantityAttributeFileName_);
		if (initialize() == false)
		{
			System.err.println("Discretization initialiazation error");
		}
	}

	public Discretizer(HDFSLineFileReader classFileReader, Hashtable classSamplinghash, HDFSLineFileReader quantityFileReader, String saveDataFolder)
	{

		preClassDataReaderOnHDFS_ = classFileReader;
		classAttributeFileName_ = classFileReader.getFileName();

		preQuantityDataReaderOnHDFS_ = quantityFileReader;
		if (quantityFileReader != null)
		{
			quantityAttributeFileName_ = quantityFileReader.getFileName();
		}

		classSamplingRatio_ = classSamplinghash;
		projectTargetFolder_ = saveDataFolder;

		// initialize entropy discretizer
		entropyDiscretizer_ = new EntropyDiscretizer(classAttributeFileName_, quantityAttributeFileName_);
		if (initializeOnHDFS() == false)
		{
			System.err.println("Discretization initialiazation error");
		}
	}

	private boolean initializeOnHDFS()
	{

		HDFSLineFileWriter quantityWriter;
		try
		{

			if (projectTargetFolder_ == null)
			{
				quantityWriter = new HDFSLineFileWriter(UtilConstants.TEMP_FOLDER + "quantity" + UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);

			} else
			{
				UtilFile.makeFolderOnHDFS(projectTargetFolder_ + UtilConstants.SUB_PATH_TOKEN + UtilConstants.HASH_DATA_FOLDER);
				quantityWriter = new HDFSLineFileWriter(projectTargetFolder_ + UtilConstants.SUB_PATH_TOKEN + UtilConstants.HASH_DATA_FOLDER + "quantity"
						+ UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
			}

			String oneClassLine = preClassDataReaderOnHDFS_.readNextLine();
			int quantityCount;
			while (oneClassLine != null)
			{
				quantityCount = 1;
				if (preQuantityDataReaderOnHDFS_ != null)
				{
					String oneQuantityLine = preQuantityDataReaderOnHDFS_.readNextLine();
					quantityCount = UtilData.convertStringToInt(oneQuantityLine);
				}

				Double n = null;
				if (classSamplingRatio_ != null)
				{
					n = (Double) classSamplingRatio_.get(String.valueOf(oneClassLine.trim()));
				}

				if (n != null)
				{
					quantityCount = quantityCount * n.intValue();
				}

				// save quantity data count to file
				quantityWriter.writeNextLine(String.valueOf(quantityCount));
				oneClassLine = preClassDataReaderOnHDFS_.readNextLine();
			}

			quantityWriter.close();

			preClassDataReaderOnHDFS_ = null;
			preQuantityDataReaderOnHDFS_ = null;
			classSamplingRatio_ = null;

		} catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}

		return true;
	}

	/**
	 * projectTargetFolder_ is used to distinguish the preview discretization or
	 * process discretization
	 */

	private boolean initialize()
	{

		AbstractProcessedColumnDataWriter quantityWriter;
		try
		{

			if (projectTargetFolder_ == null)
			{
				quantityWriter = new ProcessedColumnFileWriter(UtilConstants.TEMP_FOLDER + "quantity" + UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);

			} else
			{
				UtilFile.makeFolder(projectTargetFolder_ + UtilConstants.SUB_PATH_TOKEN + UtilConstants.HASH_DATA_FOLDER);
				quantityWriter = new ProcessedColumnFileWriter(projectTargetFolder_ + UtilConstants.SUB_PATH_TOKEN + UtilConstants.HASH_DATA_FOLDER
						+ "quantity" + UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
			}

			String oneClassLine = preClassDataReader_.readNextLine();
			int quantityCount;
			while (oneClassLine != null)
			{
				quantityCount = 1;
				if (preQuantityDataReader_ != null)
				{
					String oneQuantityLine = preQuantityDataReader_.readNextLine();
					quantityCount = UtilData.convertStringToInt(oneQuantityLine);
				}

				Double n = null;
				if (classSamplingRatio_ != null)
				{
					n = (Double) classSamplingRatio_.get(String.valueOf(oneClassLine.trim()));
				}

				if (n != null)
				{
					quantityCount = quantityCount * n.intValue();
				}

				// save quantity data count to file
				quantityWriter.writeInteger(quantityCount);
				oneClassLine = preClassDataReader_.readNextLine();
			}

			quantityWriter.close();

			preClassDataReader_ = null;
			preQuantityDataReader_ = null;
			classSamplingRatio_ = null;

		} catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public Objects discretizeEntropy(AbstractPreprocessColumnDataReader oneAttributeColumnReader, int valueType) throws Exception
	{

		String attributeFileName = oneAttributeColumnReader.getFileName();
		Objects retObjects = entropyDiscretizer_.discretizeAttributeFile(attributeFileName, valueType, 10);
		double[] splitPoints = (double[]) retObjects.getObjectByIndex(0);
		Hashtable ht_String_to_Double = retObjects.getObjectByIndexAsHASHTABLE(1);
		Hashtable ht_Double_to_String = retObjects.getObjectByIndexAsHASHTABLE(2);

		return processSplitValues(oneAttributeColumnReader, splitPoints, ht_String_to_Double, ht_Double_to_String);

	}

	public Objects discretizeEntropyOnHDFS(HDFSLineFileReader oneAttributeColumnReader, int valueType) throws Exception
	{

		String attributeFileName = oneAttributeColumnReader.getFileName();
		
		System.out.println("discretizeEntropyOnHDFS: " + attributeFileName);	
		Objects retObjects = entropyDiscretizerOnHadoop_.discretizeAttributeFileOnHDFS(attributeFileName, valueType);
		
		return retObjects;
	}

	public Objects discretizeNoneOnHDFS(HDFSLineFileReader oneAttributeColumnReader) throws Exception
	{
		Hashtable ht_Value_ElementCount = new Hashtable();
		HDFSLineFileReader quantityReader;

		if (projectTargetFolder_ == null)
		{
			quantityReader = new HDFSLineFileReader(UtilConstants.TEMP_FOLDER + "quantity" + UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
		} else
		{
			quantityReader = new HDFSLineFileReader(projectTargetFolder_ + UtilConstants.SUB_PATH_TOKEN + UtilConstants.HASH_DATA_FOLDER + "quantity"
					+ UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
		}

		String oneLine = oneAttributeColumnReader.readNextLine();

		// quantity value for each line value
		// int quantityValue = 1;
		String quantityValue = "1";
		int missingValueCount = 0;
		boolean hasMissingValue = false;

		long totalDataCount = 0;
		long readLines = 0;
		while (oneLine != null)
		{
			quantityValue = quantityReader.readNextLine();

			if (oneLine.length() == 0 || oneLine.trim().equals("?"))
			{
				hasMissingValue = true;
				missingValueCount = Integer.parseInt(quantityValue) + missingValueCount;
				oneLine = oneAttributeColumnReader.readNextLine();

				totalDataCount = totalDataCount + Integer.parseInt(quantityValue);
				readLines++;
				continue;
			}

			// get ht_Value_ElementCount
			Long I = (Long) ht_Value_ElementCount.get(oneLine.trim());
			if (I == null)
			{
				I = new Long(0);
				ht_Value_ElementCount.put(oneLine.trim(), I);
			}
			I.increaseValue(Integer.parseInt(quantityValue));

			readLines++;
			totalDataCount = totalDataCount + Integer.parseInt(quantityValue);

			oneLine = oneAttributeColumnReader.readNextLine();

		} // end of while loop

		if (hasMissingValue)
		{
			String missingValueString = "?";
			ht_Value_ElementCount.put(missingValueString, new Long(missingValueCount));
		}

		// get ElementName(actually numbers) from hashtable
		String[] elementName = new String[ht_Value_ElementCount.size()];
		String elementString;

		Enumeration keys = ht_Value_ElementCount.keys();
		int index = 0;
		while (keys.hasMoreElements())
		{
			elementString = (String) keys.nextElement();
			elementName[index] = elementString;
			index++;
		}

		Objects objects = new Objects(4);

		objects.setObjectByIndex(0, elementName);
		objects.setObjectByIndex(1, ht_Value_ElementCount);
		objects.setObjectByIndex(2, Long.valueOf(totalDataCount));
		objects.setObjectByIndex(3, Long.valueOf(readLines));
		return objects;

	}

	/**
	 * Userd for catergorial attributes which do not need to discretization
	 * 
	 */

	public Objects discretizeNone(AbstractPreprocessColumnDataReader oneAttributeColumnReader) throws Exception
	{

		Hashtable ht_Value_ElementCount = new Hashtable();
		AbstractProcessedColumnDataReader quantityReader;

		if (projectTargetFolder_ == null)
		{
			quantityReader = new ProcessedColumnFileReader(UtilConstants.TEMP_FOLDER + "quantity" + UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
		} else
		{
			quantityReader = new ProcessedColumnFileReader(projectTargetFolder_ + UtilConstants.SUB_PATH_TOKEN + UtilConstants.HASH_DATA_FOLDER + "quantity"
					+ UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
		}

		String oneLine = oneAttributeColumnReader.readNextLine();

		// quantity value for each line value
		int quantityValue = 1;
		int missingValueCount = 0;
		boolean hasMissingValue = false;

		long totalDataCount = 0;
		long readLines = 0;
		while (oneLine != null)
		{
			quantityValue = quantityReader.readNextInteger();

			if (oneLine.length() == 0 || oneLine.trim().equals("?"))
			{
				hasMissingValue = true;
				missingValueCount = quantityValue + missingValueCount;
				oneLine = oneAttributeColumnReader.readNextLine();

				totalDataCount = totalDataCount + quantityValue;
				readLines++;
				continue;
			}

			// get ht_Value_ElementCount
			Long I = (Long) ht_Value_ElementCount.get(oneLine.trim());
			if (I == null)
			{
				I = new Long(0);
				ht_Value_ElementCount.put(oneLine.trim(), I);
			}
			I.increaseValue(quantityValue);

			readLines++;
			totalDataCount = totalDataCount + quantityValue;

			oneLine = oneAttributeColumnReader.readNextLine();

		} // end of while loop

		if (hasMissingValue)
		{
			String missingValueString = "?";
			ht_Value_ElementCount.put(missingValueString, new Long(missingValueCount));
		}

		// get ElementName(actually numbers) from hashtable
		String[] elementName = new String[ht_Value_ElementCount.size()];
		String elementString;

		Enumeration keys = ht_Value_ElementCount.keys();
		int index = 0;
		while (keys.hasMoreElements())
		{
			elementString = (String) keys.nextElement();
			elementName[index] = elementString;
			index++;
		}

		Objects objects = new OmObjects(4);

		objects.setObjectByIndex(0, elementName);
		objects.setObjectByIndex(1, ht_Value_ElementCount);
		objects.setObjectByIndex(2, Long.valueOf(totalDataCount));
		objects.setObjectByIndex(3, Long.valueOf(readLines));
		return objects;

	}

	public Objects discretizeManualBins(AbstractPreprocessColumnDataReader oneAttributeReader, Vector additionInfoVector) throws Exception
	{

		double[] d = new double[additionInfoVector.size()];
		for (int i = 0; i < additionInfoVector.size(); i++)
		{
			d[i] = Double.valueOf((String) additionInfoVector.get(i)).doubleValue();
		}

		return processSplitValues(oneAttributeReader, d, null, null);
	}

	public Objects discretizeManualBinsOnHDFS(HDFSLineFileReader oneAttributeReader, Vector additionInfoVector) throws Exception
	{

		double[] d = new double[additionInfoVector.size()];
		for (int i = 0; i < additionInfoVector.size(); i++)
		{
			d[i] = Double.valueOf((String) additionInfoVector.get(i)).doubleValue();
		}

		return processSplitValuesOnHDFS(oneAttributeReader, d, null, null);
	}

	public Objects discretizeFixedEvenBinNumberOnHDFS(HDFSLineFileReader oneAttributeReader, int binCount) throws Exception
	{

		Vector<Double> ret = getFixedEvenBinSplitValueOnHDFS(oneAttributeReader, binCount);

		double[] d = new double[ret.size()];
		for (int i = 0; i < ret.size(); i++)
		{
			d[i] = ret.get(i).doubleValue();
		}

		return processSplitValuesOnHDFS(oneAttributeReader, d, null, null);

	}

	public Objects discretizeFixedEvenBinNumber(AbstractPreprocessColumnDataReader oneAttributeReader, int binCount) throws Exception
	{

		Vector<Double> ret = getFixedEvenBinSplitValue(oneAttributeReader, binCount);

		double[] d = new double[ret.size()];
		for (int i = 0; i < ret.size(); i++)
		{
			d[i] = ret.get(i).doubleValue();
		}

		return processSplitValues(oneAttributeReader, d, null, null);

	}

	private Vector<Double> getFixedEvenBinSplitValueOnHDFS(HDFSLineFileReader oneAttributeReader, int binCount)
	{

		HDFSLineFileReader quantityReader;
		/* distinguish preview and project discretizaiton */
		if (projectTargetFolder_ == null)
		{
			quantityReader = new HDFSLineFileReader(UtilConstants.TEMP_FOLDER + "quantity" + UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
		}

		else
		{
			quantityReader = new HDFSLineFileReader(projectTargetFolder_ + UtilConstants.SUB_PATH_TOKEN + UtilConstants.HASH_DATA_FOLDER + "quantity"
					+ UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
		}

		// find all values
		long totalDataCount = 0;
		Vector<OmObjects> values = new Vector<OmObjects>();
		try
		{
			String oneLine = oneAttributeReader.readNextLine();
//			int oneQuantityLine = 1;
            String oneQuantityLine = "1"; 
			
			while (oneLine != null)
			{
				try
				{
					double oneValue = Double.parseDouble(oneLine);
					oneQuantityLine = quantityReader.readNextLine();

					OmObjects ob = new OmObjects(2);
					ob.setObjectByIndex(0, oneValue);
					ob.setObjectByIndex(1, oneQuantityLine);

					values.add(ob);

					totalDataCount = totalDataCount + Integer.parseInt(oneQuantityLine);

				}

				catch (NumberFormatException ex2)
				{
					oneQuantityLine = quantityReader.readNextLine();
					oneLine = oneAttributeReader.readNextLine();
					continue;

				}

				oneLine = oneAttributeReader.readNextLine();

			}
		}

		catch (Exception ex)
		{
			ex.printStackTrace();
		}

		// reread oneAttribute and close quantity
		try
		{
			oneAttributeReader.restartFromFirstLine();
			quantityReader.close();
		} catch (Exception ex1)
		{
			ex1.printStackTrace();
		}

		// sort all values
		Collections.sort(values, new Comparator()
		{
			public int compare(Object o1, Object o2)
			{
				Double D1 = (Double) ((OmObjects) o1).getObjectByIndex(0);
				Double D2 = (Double) ((OmObjects) o2).getObjectByIndex(0);
				return D1.compareTo(D2);
			}
		});

		long eachBinCount = totalDataCount / binCount;
		Vector<Double> splitValues = new Vector<Double>(binCount);
		long count = 0;
		for (int i = 0; i < values.size(); i++)
		{
			OmObjects ob = (OmObjects) values.get(i);
			Double oneValue = (Double) ob.getObjectByIndex(0);
			Integer oneQuantity = (Integer) ob.getObjectByIndex(1);
			if (count > eachBinCount)
			{
				if (splitValues.indexOf(oneValue) < 0)
				{
					splitValues.add(oneValue);
				}
				count = 0;
			}
			count = count + oneQuantity.intValue();
		}

		return splitValues;

	}

	private Vector<Double> getFixedEvenBinSplitValue(AbstractPreprocessColumnDataReader oneAttributeReader, int binCount)
	{

		AbstractProcessedColumnDataReader quantityReader;
		/* distinguish preview and project discretizaiton */
		if (projectTargetFolder_ == null)
		{
			quantityReader = new ProcessedColumnFileReader(UtilConstants.TEMP_FOLDER + "quantity" + UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
		}

		else
		{
			quantityReader = new ProcessedColumnFileReader(projectTargetFolder_ + UtilConstants.SUB_PATH_TOKEN + UtilConstants.HASH_DATA_FOLDER + "quantity"
					+ UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
		}

		// find all values
		long totalDataCount = 0;
		Vector<Objects> values = new Vector<OmObjects>();
		try
		{
			String oneLine = oneAttributeReader.readNextLine();
			int oneQuantityLine = 1;

			while (oneLine != null)
			{
				try
				{
					double oneValue = Double.parseDouble(oneLine);
					oneQuantityLine = quantityReader.readNextInteger();

					Objects ob = new Objects(2);
					ob.setObjectByIndex(0, oneValue);
					ob.setObjectByIndex(1, oneQuantityLine);

					values.add(ob);

					totalDataCount = totalDataCount + oneQuantityLine;

				}

				catch (NumberFormatException ex2)
				{
					oneQuantityLine = quantityReader.readNextInteger();
					oneLine = oneAttributeReader.readNextLine();
					continue;

				}

				oneLine = oneAttributeReader.readNextLine();

			}
		}

		catch (Exception ex)
		{
			ex.printStackTrace();
		}

		// reread oneAttribute and close quantity
		try
		{
			oneAttributeReader.restartFromFirstLine();
			quantityReader.close();
		} catch (Exception ex1)
		{
			ex1.printStackTrace();
		}

		// sort all values
		Collections.sort(values, new Comparator()
		{
			public int compare(Object o1, Object o2)
			{
				Double D1 = (Double) ((OmObjects) o1).getObjectByIndex(0);
				Double D2 = (Double) ((OmObjects) o2).getObjectByIndex(0);
				return D1.compareTo(D2);
			}
		});

		long eachBinCount = totalDataCount / binCount;
		Vector<Double> splitValues = new Vector<Double>(binCount);
		long count = 0;
		for (int i = 0; i < values.size(); i++)
		{
			Objects ob = (Objects) values.get(i);
			Double oneValue = (Double) ob.getObjectByIndex(0);
			Integer oneQuantity = (Integer) ob.getObjectByIndex(1);
			if (count > eachBinCount)
			{
				if (splitValues.indexOf(oneValue) < 0)
				{
					splitValues.add(oneValue);
				}
				count = 0;
			}
			count = count + oneQuantity.intValue();
		}

		return splitValues;

	}

	private Objects processSplitValuesOnHDFS(HDFSLineFileReader oneAttributeReader, double[] splitValues, Hashtable ht_String_to_Double,
			Hashtable ht_Double_to_String) throws Exception
	{
		/* each range contains how many elements */
		Hashtable ht_Range_ElementCount = new Hashtable();
		HDFSLineFileReader quantityReader = null;

		/* distinguish preview and project discretizaiton */
		if (projectTargetFolder_ == null)
		{
			quantityReader = new HDFSLineFileReader(UtilConstants.TEMP_FOLDER + "quantity" + UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
		}

		else
		{
			quantityReader = new HDFSLineFileReader(projectTargetFolder_ + UtilConstants.SUB_PATH_TOKEN + UtilConstants.HASH_DATA_FOLDER + "quantity"
					+ UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
		}

		Arrays.sort(splitValues);

		byte[] NegInf = new byte[] { '(', '-', 'i', 'n', 'f' };
		byte[] PovInf = new byte[] { '+', 'i', 'n', 'f', ')' };
		byte[] divider = new byte[] { ';' };
		byte[] LeftB = new byte[] { '[' };
		byte[] RightB = new byte[] { ')' };

		String[] rangeNames = new String[splitValues.length + 1];

		// no split point
		if (splitValues.length == 0)
		{
			byte[] range = UtilData.appendByteArray(UtilData.appendByteArray(NegInf, divider), PovInf);
			rangeNames[0] = new String(range);
		}

		else
		{
			byte[] range = UtilData.appendByteArray(NegInf, divider);

			// value type is "Double String"
			if (ht_Double_to_String == null)
			{

				range = UtilData.appendByteArray(range, new Double(splitValues[0]).toString().getBytes());
				range = UtilData.appendByteArray(range, RightB);
				rangeNames[0] = new String(range);

				for (int j = 1; j < splitValues.length; j++)
				{
					range = null;
					range = UtilData.appendByteArray(LeftB, new Double(splitValues[j - 1]).toString().getBytes());
					range = UtilData.appendByteArray(range, divider);
					range = UtilData.appendByteArray(range, new Double(splitValues[j]).toString().getBytes());
					range = UtilData.appendByteArray(range, RightB);

					rangeNames[j] = new String(range);
				}

				range = null;
				range = UtilData.appendByteArray(LeftB, new Double(splitValues[splitValues.length - 1]).toString().getBytes());
				range = UtilData.appendByteArray(range, divider);
				range = UtilData.appendByteArray(range, PovInf);

				rangeNames[splitValues.length] = new String(range);
			}

			// value type is not not "Double String"
			else
			{

				String leftMost = (String) ht_Double_to_String.get(new Double(splitValues[0]));
				range = UtilData.appendByteArray(range, leftMost.getBytes());
				range = UtilData.appendByteArray(range, RightB);
				rangeNames[0] = new String(range);

				for (int j = 1; j < splitValues.length; j++)
				{
					range = null;
					String leftValue = (String) ht_Double_to_String.get(new Double(splitValues[j - 1]));
					range = UtilData.appendByteArray(LeftB, leftValue.getBytes());
					range = UtilData.appendByteArray(range, divider);

					String rightValue = (String) ht_Double_to_String.get(new Double(splitValues[j]));
					range = UtilData.appendByteArray(range, rightValue.getBytes());
					range = UtilData.appendByteArray(range, RightB);

					rangeNames[j] = new String(range);
				}

				range = null;
				String lastValue = (String) ht_Double_to_String.get(new Double(splitValues[splitValues.length - 1]));
				range = UtilData.appendByteArray(LeftB, lastValue.getBytes());
				range = UtilData.appendByteArray(range, divider);
				range = UtilData.appendByteArray(range, PovInf);

				rangeNames[splitValues.length] = new String(range);
			}
		}

		String oneLine = oneAttributeReader.readNextLine();
		// quantity value for each line value
		String quantityValue = "1";
		// the last range index
		int hashIndex = splitValues.length;
		int missingValueCount = 0;
		boolean hasMissingValue = false;

		long readLineCount = 0;
		long totalDataCount = 0;

		while (oneLine != null)
		{
			quantityValue = quantityReader.readNextLine();
			// meet missing value
			if (oneLine.length() == 0 || oneLine.trim().equals("?"))
			{
				hasMissingValue = true;
				missingValueCount = missingValueCount + Integer.parseInt(quantityValue);

				totalDataCount = totalDataCount + Integer.parseInt(quantityValue);
				readLineCount++;

				oneLine = oneAttributeReader.readNextLine();

				continue;
			}

			else
			{
				double d;
				try
				{
					if (ht_Double_to_String == null)
					{
						d = Double.parseDouble(oneLine);
					}

					else
					{
						d = ((Double) ht_String_to_Double.get(oneLine)).doubleValue();
					}
				} catch (NumberFormatException ex)
				{

					hasMissingValue = true;
					missingValueCount = Integer.parseInt(quantityValue) + missingValueCount;
					oneLine = oneAttributeReader.readNextLine();

					totalDataCount = totalDataCount + Integer.parseInt(quantityValue);
					readLineCount++;
					continue;
				}

				// find range
				int i = 0;
				for (; i < splitValues.length; i++)
				{
					if (d < splitValues[i])
					{
						hashIndex = i;
						break;
					}
				}

				if (i == splitValues.length)
				{
					hashIndex = i;
				}

				// get ht_Range_ElementCount
				OmLong I = (OmLong) ht_Range_ElementCount.get(rangeNames[hashIndex]);
				if (I == null)
				{
					I = new OmLong(0);
					ht_Range_ElementCount.put(rangeNames[hashIndex], I);
				}
				I.increaseValue(Integer.parseInt(quantityValue));

				oneLine = oneAttributeReader.readNextLine();

				totalDataCount = totalDataCount + Integer.parseInt(quantityValue);
				readLineCount++;

			} // end of else
		} // end of while

		// if existing missing value, we need to modify rangeNames and
		// ht_Range_ElementCount
		if (hasMissingValue)
		{

			String missingValueString = "?";
			String[] tempRangeNames = new String[rangeNames.length + 1];
			tempRangeNames[0] = "?";
			for (int i = 0; i < rangeNames.length; i++)
			{
				tempRangeNames[i + 1] = rangeNames[i];
			}
			rangeNames = tempRangeNames;

			ht_Range_ElementCount.put(missingValueString, new OmLong(missingValueCount));

		}

		Objects objects = new Objects(7);

		Vector splitValuesVector = new Vector(splitValues.length);
		for (int j = 0; j < splitValues.length; j++)
		{
			splitValuesVector.add(Double.toString(splitValues[j]));
		}

		if (rangeNames.length != ht_Range_ElementCount.size())
		{
			for (int i = 0; i < rangeNames.length; i++)
			{
				Long I = (Long) ht_Range_ElementCount.get(rangeNames[i]);
				if (I == null)
				{
					I = new OmLong(0);
					ht_Range_ElementCount.put(rangeNames[i], I);
				}
			}
		}

		objects.setObjectByIndex(0, rangeNames);
		objects.setObjectByIndex(1, ht_Range_ElementCount);
		objects.setObjectByIndex(2, Long.valueOf(totalDataCount));
		objects.setObjectByIndex(3, Long.valueOf(readLineCount));
		objects.setObjectByIndex(4, splitValuesVector); // split points
		objects.setObjectByIndex(5, ht_String_to_Double);
		objects.setObjectByIndex(6, ht_Double_to_String);

		return objects;

	}

	/**
	 * 1. create a new ProcessedColumnFileWriter 2. read from
	 * oneAttributeReader, and using the splitValues, hash them, and write the
	 * hashed int to file. 3. return a ProcessedColumnFileReader, and the hash
	 * table. For example, if oneAttributeReader gives us values: "10", "14.3",
	 * "38", "5", "1", and the splitValues[] are: {13, 20} then the values will
	 * be (as String format): (-inf,13), [13, 20), [20, +inf), (-inf,13),
	 * (-inf,13) now, we hash the Strings to int, such as: 1, 2, 3, 1, 1 (and
	 * saved as processed column file format).
	 */

	private Objects processSplitValues(AbstractPreprocessColumnDataReader oneAttributeReader, double[] splitValues, Hashtable ht_String_to_Double,
			Hashtable ht_Double_to_String) throws Exception
	{

		/* each range contains how many elements */
		Hashtable ht_Range_ElementCount = new Hashtable();
		AbstractProcessedColumnDataReader quantityReader = null;

		/* distinguish preview and project discretizaiton */
		if (projectTargetFolder_ == null)
		{
			quantityReader = new ProcessedColumnFileReader(UtilConstants.TEMP_FOLDER + "quantity" + UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
		}

		else
		{
			quantityReader = new ProcessedColumnFileReader(projectTargetFolder_ + UtilConstants.SUB_PATH_TOKEN + UtilConstants.HASH_DATA_FOLDER + "quantity"
					+ UtilConstants.INIT_DISCRETIZE_FILE_NAME_EXTENSION);
		}

		Arrays.sort(splitValues);

		byte[] NegInf = new byte[] { '(', '-', 'i', 'n', 'f' };
		byte[] PovInf = new byte[] { '+', 'i', 'n', 'f', ')' };
		byte[] divider = new byte[] { ';' };
		byte[] LeftB = new byte[] { '[' };
		byte[] RightB = new byte[] { ')' };

		String[] rangeNames = new String[splitValues.length + 1];

		// no split point
		if (splitValues.length == 0)
		{
			byte[] range = UtilData.appendByteArray(UtilData.appendByteArray(NegInf, divider), PovInf);
			rangeNames[0] = new String(range);
		}

		else
		{
			byte[] range = UtilData.appendByteArray(NegInf, divider);

			// value type is "Double String"
			if (ht_Double_to_String == null)
			{

				range = UtilData.appendByteArray(range, new Double(splitValues[0]).toString().getBytes());
				range = UtilData.appendByteArray(range, RightB);
				rangeNames[0] = new String(range);

				for (int j = 1; j < splitValues.length; j++)
				{
					range = null;
					range = UtilData.appendByteArray(LeftB, new Double(splitValues[j - 1]).toString().getBytes());
					range = UtilData.appendByteArray(range, divider);
					range = UtilData.appendByteArray(range, new Double(splitValues[j]).toString().getBytes());
					range = UtilData.appendByteArray(range, RightB);

					rangeNames[j] = new String(range);
				}

				range = null;
				range = UtilData.appendByteArray(LeftB, new Double(splitValues[splitValues.length - 1]).toString().getBytes());
				range = UtilData.appendByteArray(range, divider);
				range = UtilData.appendByteArray(range, PovInf);

				rangeNames[splitValues.length] = new String(range);
			}

			// value type is not not "Double String"
			else
			{

				String leftMost = (String) ht_Double_to_String.get(new Double(splitValues[0]));
				range = UtilData.appendByteArray(range, leftMost.getBytes());
				range = UtilData.appendByteArray(range, RightB);
				rangeNames[0] = new String(range);

				for (int j = 1; j < splitValues.length; j++)
				{
					range = null;
					String leftValue = (String) ht_Double_to_String.get(new Double(splitValues[j - 1]));
					range = UtilData.appendByteArray(LeftB, leftValue.getBytes());
					range = UtilData.appendByteArray(range, divider);

					String rightValue = (String) ht_Double_to_String.get(new Double(splitValues[j]));
					range = UtilData.appendByteArray(range, rightValue.getBytes());
					range = UtilData.appendByteArray(range, RightB);

					rangeNames[j] = new String(range);
				}

				range = null;
				String lastValue = (String) ht_Double_to_String.get(new Double(splitValues[splitValues.length - 1]));
				range = UtilData.appendByteArray(LeftB, lastValue.getBytes());
				range = UtilData.appendByteArray(range, divider);
				range = UtilData.appendByteArray(range, PovInf);

				rangeNames[splitValues.length] = new String(range);
			}
		}

		String oneLine = oneAttributeReader.readNextLine();
		// quantity value for each line value
		int quantityValue = 1;
		// the last range index
		int hashIndex = splitValues.length;
		int missingValueCount = 0;
		boolean hasMissingValue = false;

		long readLineCount = 0;
		long totalDataCount = 0;

		while (oneLine != null)
		{
			quantityValue = quantityReader.readNextInteger();
			// meet missing value
			if (oneLine.length() == 0 || oneLine.trim().equals("?"))
			{
				hasMissingValue = true;
				missingValueCount = missingValueCount + quantityValue;

				totalDataCount = totalDataCount + quantityValue;
				readLineCount++;

				oneLine = oneAttributeReader.readNextLine();

				continue;
			}

			else
			{
				double d;
				try
				{
					if (ht_Double_to_String == null)
					{
						d = Double.parseDouble(oneLine);
					}

					else
					{
						d = ((Double) ht_String_to_Double.get(oneLine)).doubleValue();
					}
				} catch (NumberFormatException ex)
				{

					hasMissingValue = true;
					missingValueCount = quantityValue + missingValueCount;
					oneLine = oneAttributeReader.readNextLine();

					totalDataCount = totalDataCount + quantityValue;
					readLineCount++;
					continue;
				}

				// find range
				int i = 0;
				for (; i < splitValues.length; i++)
				{
					if (d < splitValues[i])
					{
						hashIndex = i;
						break;
					}
				}

				if (i == splitValues.length)
				{
					hashIndex = i;
				}

				// get ht_Range_ElementCount
				OmLong I = (OmLong) ht_Range_ElementCount.get(rangeNames[hashIndex]);
				if (I == null)
				{
					I = new OmLong(0);
					ht_Range_ElementCount.put(rangeNames[hashIndex], I);
				}
				I.increaseValue(quantityValue);

				oneLine = oneAttributeReader.readNextLine();

				totalDataCount = totalDataCount + quantityValue;
				readLineCount++;

			} // end of else
		} // end of while

		// if existing missing value, we need to modify rangeNames and
		// ht_Range_ElementCount
		if (hasMissingValue)
		{

			String missingValueString = "?";
			String[] tempRangeNames = new String[rangeNames.length + 1];
			tempRangeNames[0] = "?";
			for (int i = 0; i < rangeNames.length; i++)
			{
				tempRangeNames[i + 1] = rangeNames[i];
			}
			rangeNames = tempRangeNames;

			ht_Range_ElementCount.put(missingValueString, new OmLong(missingValueCount));

		}

		Objects objects = new Objects(7);

		Vector splitValuesVector = new Vector(splitValues.length);
		for (int j = 0; j < splitValues.length; j++)
		{
			splitValuesVector.add(Double.toString(splitValues[j]));
		}

		if (rangeNames.length != ht_Range_ElementCount.size())
		{
			for (int i = 0; i < rangeNames.length; i++)
			{
				OmLong I = (OmLong) ht_Range_ElementCount.get(rangeNames[i]);
				if (I == null)
				{
					I = new OmLong(0);
					ht_Range_ElementCount.put(rangeNames[i], I);
				}
			}
		}

		objects.setObjectByIndex(0, rangeNames);
		objects.setObjectByIndex(1, ht_Range_ElementCount);
		objects.setObjectByIndex(2, Long.valueOf(totalDataCount));
		objects.setObjectByIndex(3, Long.valueOf(readLineCount));
		objects.setObjectByIndex(4, splitValuesVector); // split points
		objects.setObjectByIndex(5, ht_String_to_Double);
		objects.setObjectByIndex(6, ht_Double_to_String);

		return objects;
	}

	public static void main(String[] args)
	{

		// PreprocessColumnFileReader fileReader = new
		// PreprocessColumnFileReader("c:\\11.wri");
		// PreprocessColumnFileReader quantityReader = new
		// PreprocessColumnFileReader("c:\\15.wri");
		// PreprocessColumnFileReader attributeReader = new
		// PreprocessColumnFileReader("c:\\0.wri");
		//
		// Discretizer dis = new Discretizer(fileReader,null, quantityReader,0,
		// null);
		// try
		// {
		// OmObjects obs = dis.discretizeEntropy(attributeReader,
		// UtilConstants.VALUE_TYPE_DOUBLE_NUMBER_STRING);
		// }
		//catch (Exception ex)
		//{
		//}
		int i = 8;
		int j = 5;

		double d = 1.0 * i / j;

		System.out.println(d);
	}

} // end of class
