package com.dataiku.dip.plugins.anonymizer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;

import com.dataiku.dip.datalayer.Column;
import com.dataiku.dip.datalayer.Processor;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datalayer.SingleRowProcessor;
import com.dataiku.dip.shaker.model.StepParams;
import com.dataiku.dip.shaker.processors.Category;
import com.dataiku.dip.shaker.processors.ProcessorMeta;
import com.dataiku.dip.shaker.processors.ProcessorTag;
import com.dataiku.dip.shaker.server.ProcessorDesc;
import com.dataiku.dip.shaker.text.StringNormalizer;
import com.dataiku.dip.util.ParamDesc;
import com.dataiku.dip.utils.JSON;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class AnonymizerProcessor extends SingleRowProcessor implements Processor {

    static class AnonymizationDictMapper {
        private List<String> dict;
        private List<Set<String>> alreadyDefined = new ArrayList<>();
        private boolean strictMode;
        private HashFunction hashFunc = Hashing.goodFastHash(32);
        boolean slugOutput;

        AnonymizationDictMapper(List<String> dict, boolean strictMode, boolean slugOutput) {
            this.dict = dict;
            this.strictMode = strictMode; 
            this.slugOutput = slugOutput;
        }
        String process(String input) {
            int hash = Math.abs(hashFunc.hashString(input, StandardCharsets.UTF_8).asInt());
            int bucket = hash % dict.size();
            
            String dictEntry = dict.get(bucket);
            
            if (strictMode) {
                while (alreadyDefined.size() <= bucket) alreadyDefined.add(new HashSet<String>());
                
                Set<String> alreadyBucket = alreadyDefined.get(bucket);
                alreadyBucket.add(input);
                dictEntry = dictEntry + "-" + alreadyBucket.size();
            }
            if (slugOutput) {
                dictEntry = StringNormalizer.normalize(dictEntry.toLowerCase()).replaceAll("[^A-Za-z0-9_]", "_");
            }
            return dictEntry;
        }
    }

    public enum ProcessingMode {
        REGULAR,
        EMAIL,
        ARRAY
    }

    public static class Parameter implements StepParams {
        private static final long serialVersionUID = -1;

        String inputColumn;
        String outputColumn;

        ProcessingMode processingMode = ProcessingMode.REGULAR;

        StandardDict dictionary = StandardDict.STAR_WARS_PLANETS;

        String customDict;
        
        boolean strictMode;
        boolean slugOutput;

        @Override
        public void validate() throws IllegalArgumentException {}
    }

    public static ProcessorMeta<AnonymizerProcessor, Parameter> META = new ProcessorMeta<AnonymizerProcessor, Parameter>() {
        @Override
        public String getName() {
            return "AnonymizerProcessor";
        }

        @Override
        public Category getCategory() {
            return Category.WEB;
        }
        @Override
        public Set<ProcessorTag> getTags() {
            return Sets.newHashSet(ProcessorTag.WEB, ProcessorTag.ENRICH);
        }

        @Override
        public String getHelp() {
            return "\n";
        }

        @Override
        public Class<Parameter> stepParamClass() {
            return Parameter.class;
        }

        @Override
        public ProcessorDesc describe() {
            return ProcessorDesc
                    .withGenericForm(this.getName(), "Anonymize data")
                    .withMNEColParam("inputColumn", "Input column")
                    .withParam("outputColumn", "string", false, true, "Output column (Empty for in-place)")
                    .withParam(
                            ParamDesc.advancedSelect("dictionary", "Dictionary", "Anonymization dictionary", 
                                    new String[]{ "STAR_WARS_PLANETS", "DWARF_NAMES", "CUSTOM"},
                                    new String[]{
                                    "Star Wars planets", "Dwarf names", "Custom"}).withDefaultValue("STAR_WARS_PLANETS"))
                                    .withParam(ParamDesc.textarea("customDict", "Custom Dictionary (one entry per line)").withMandatory(false).withCanBeEmpty(true))
                                    .withParam(
                                            ParamDesc.advancedSelect("processingMode", "Mode", "Processing mode", 
                                                    new String[]{ "REGULAR", "EMAIL", "ARRAY"},
                                                    new String[]{
                                                    "Regular", "E-mail (keep domain)", "Array (each element)"}).withDefaultValue("REGULAR"))
                                            .withBool("strictMode", "No collisions", "Uses more memory")
                                            .withBool("slugOutput", "Slug-like output", "(like_that)")
                                            .deprecate()
                    .withReplacementDocLink("preparation/processors/column-pseudonymization")
                    .withReplacementName("Pseudonymize text");

        }
        @Override
        public AnonymizerProcessor build(Parameter parameter) throws Exception {
            return new AnonymizerProcessor(parameter);
        }
    };

    public AnonymizerProcessor(Parameter parameter) {
        this.parameter = parameter;
    }

    private Column inputCD, outputCD;
    private AnonymizationDictMapper mapper;

    public void init() throws Exception {
        inputCD = cf.column(parameter.inputColumn);
        if (StringUtils.isBlank(parameter.outputColumn)) {
            outputCD = inputCD;
        } else {
            outputCD = cf.columnAfter(parameter.inputColumn, parameter.outputColumn);
        }

        switch (parameter.dictionary) {
        case CUSTOM:
            List<String> lines = new ArrayList<>();
            for (String line : StringUtils.split(parameter.customDict, "\n")) {
                lines.add(line);
            }
            this.mapper = new AnonymizationDictMapper(lines, parameter.strictMode, parameter.slugOutput);
            break;
        default:
            this.mapper = new AnonymizationDictMapper(parameter.dictionary.getData(), parameter.strictMode, parameter.slugOutput);
        }
    }

    @Override
    public void processRow(Row row) throws Exception {
        String inputData = row.get(inputCD);

        if (StringUtils.isBlank(inputData)) return;

        switch (parameter.processingMode) {
        case REGULAR:
            row.put(outputCD, mapper.process(inputData));
            break;
        case EMAIL:
            String[] parts = inputData.split("@");

            if (parts.length != 2) {
                row.put(outputCD, "???");
            }
            row.put(outputCD, mapper.process(parts[0]) + "@"  + parts[1]);
            break;

        case ARRAY:
            try {
                List<String> ret = new ArrayList<>();
                JSONArray array = new JSONArray(inputData);
                for (int i = 0; i < array.length(); i++) {
                    String s = array.getString(i);
                    if (!StringUtils.isBlank(s)) {
                        ret.add(mapper.process(s));
                    }
                }
                row.put(outputCD, JSON.json(ret));
            } catch (Exception e) {
                row.put(outputCD, "??");
            }
            break;
        }
    }

    @Override
    public void postProcess() {
    }

    final Parameter parameter;
}