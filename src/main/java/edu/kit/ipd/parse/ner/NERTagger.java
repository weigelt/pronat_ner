package edu.kit.ipd.parse.ner;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.kit.ipd.parse.luna.data.AbstractPipelineData;
import edu.kit.ipd.parse.luna.data.MissingDataException;
import edu.kit.ipd.parse.luna.data.PipelineDataCastException;
import edu.kit.ipd.parse.luna.data.PrePipelineData;
import edu.kit.ipd.parse.luna.data.token.Token;
import edu.kit.ipd.parse.luna.pipeline.IPipelineStage;
import edu.kit.ipd.parse.luna.pipeline.PipelineStageException;
import edu.kit.ipd.parse.luna.tools.ConfigManager;
import edu.kit.ipd.parse.senna_wrapper.Senna;
import edu.kit.ipd.parse.senna_wrapper.WordSennaResult;

/**
 * This class represents a {@link IPipelineStage} which adds possible Named
 * Entity Recognition tags to the Graph
 *
 * @author Tobias Hey
 * @author Sebastian Weigelt - changed in/out from graph to token on 2016-09-12
 *
 */
@MetaInfServices(IPipelineStage.class)
public class NERTagger implements IPipelineStage {

	/**
	 * The {@link String} representation of the NER attribute
	 */
	public static final String NER_ATTRIBUTE_NAME = "ner";

	private static final Logger logger = LoggerFactory.getLogger(NERTagger.class);

	private static final String ID = "ner";

	private PrePipelineData prePipeData;

	private Senna senna;

	private Properties props;

	private boolean parsePerInstruction;

	@Override
	public void init() {
		props = ConfigManager.getConfiguration(getClass());
		parsePerInstruction = Boolean.parseBoolean(props.getProperty("PARSE_PER_INSTRUCTION"));
		senna = new Senna(new String[] { "-usrtokens", "-ner" });

	}

	@Override
	public void exec(AbstractPipelineData data) throws PipelineStageException {
		try {
			prePipeData = data.asPrePipelineData();
		} catch (final PipelineDataCastException e) {
			logger.error("Cannot process on data - PipelineData unreadable", e);
			throw new PipelineStageException(e);
		}

		final List<List<Token>> taggedHypotheses;

		try {
			taggedHypotheses = prePipeData.getTaggedHypotheses();
		} catch (final MissingDataException e1) {
			logger.error("No tagged hypotheses provided", e1);
			throw new PipelineStageException(e1);
		}

		for (final List<Token> taggedHypothesis : taggedHypotheses) {
			List<WordSennaResult> result;
			try {
				result = parse(taggedHypothesis);
			} catch (final IOException e) {
				logger.error("An IOException occured during run of SENNA", e);
				throw new PipelineStageException(e);
			} catch (final URISyntaxException e) {
				logger.error("An URISyntaxException occured during initialization of SENNA", e);
				throw new PipelineStageException(e);
			} catch (final InterruptedException e) {
				logger.error("The SENNA process interrupted unexpectedly", e);
				throw new PipelineStageException(e);
			}
			putResultIntoToken(result, taggedHypothesis);
		}

	}

	private void putResultIntoToken(List<WordSennaResult> result, List<Token> taggedHypothesis) {
		if (result.size() != taggedHypothesis.size()) {
			throw new IllegalArgumentException("Senna Result and hypothesis differ in length!");
		}
		for (int i = 0; i < taggedHypothesis.size(); i++) {
			taggedHypothesis.get(i).setNer(result.get(i).getAnalysisResults()[0]);

		}
	}

	/**
	 * This method parses the specified tokens with SENNA and returns the
	 * contained words associated with their NER Tags
	 *
	 * @param tokens
	 *            The tokenss to process
	 * @return the words and their ner Tags as {@link WordSennaResult}
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 */
	public List<WordSennaResult> parse(List<Token> tokens) throws IOException, URISyntaxException, InterruptedException {
		List<WordSennaResult> result = new ArrayList<>();

		if (parsePerInstruction) {
			logger.info("parsing NER for each instruction independently");
			final List<String> inputList = generateInstructionInput(tokens);

			for (final String input : inputList) {
				final File inputTmpFile = writeToTempFile(input);
				result.addAll(senna.parse(inputTmpFile));

			}
		} else {
			String input = "";
			for (final Token t : tokens) {
				input += t.getWord() + " ";
			}
			final File inputTmpFile = writeToTempFile(input);
			logger.info("parsing NER without instructions");
			result = senna.parse(inputTmpFile);
		}
		return result;
	}

	private List<String> generateInstructionInput(List<Token> tokens) {
		final List<String> inputList = new ArrayList<>();
		int instructionNumber = tokens.get(0).getInstructionNumber();
		String instruction = "";
		for (final Token t : tokens) {

			if (t.getInstructionNumber() > instructionNumber) {
				inputList.add(instruction);
				instruction = "";
			}
			instruction += t.getWord() + " ";
			instructionNumber = t.getInstructionNumber();
		}
		inputList.add(instruction);
		return inputList;
	}

	/**
	 * This method writes the input text into a text file. The text file is the
	 * input file for SENNA.
	 *
	 * @param text
	 *            the text to parse
	 * @throws IOException
	 */
	private File writeToTempFile(String text) throws IOException {
		PrintWriter writer;
		final File tempFile = File.createTempFile("input", "txt");
		writer = new PrintWriter(tempFile);
		writer.println(text);
		writer.close();
		return tempFile;

	}

	@Override
	public String getID() {
		return ID;
	}

}
