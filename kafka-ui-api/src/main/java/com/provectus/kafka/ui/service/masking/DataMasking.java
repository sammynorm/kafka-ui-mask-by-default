package com.provectus.kafka.ui.service.masking;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.provectus.kafka.ui.config.ClustersProperties;
import com.provectus.kafka.ui.model.TopicMessageDTO;
import com.provectus.kafka.ui.serde.api.Serde;
import com.provectus.kafka.ui.service.masking.policies.MaskingPolicy;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataMasking {
  private static final Logger log = LoggerFactory.getLogger(DataMasking.class);

  private static final JsonMapper JSON_MAPPER = new JsonMapper();

  private boolean maskByDefault;

  @Value
  static class Mask {
    @Nullable
    Pattern topicKeysPattern;
    @Nullable
    Pattern topicValuesPattern;

    MaskingPolicy policy;

    boolean shouldBeApplied(String topic, Serde.Target target) {
      return target == Serde.Target.KEY
          ? topicKeysPattern != null && topicKeysPattern.matcher(topic).matches()
          : topicValuesPattern != null && topicValuesPattern.matcher(topic).matches();
    }
  }

  private final List<Mask> masks;

  public static DataMasking create(ClustersProperties.Cluster clusterConfig) {
    List<Mask> masks = Optional.ofNullable(clusterConfig.getMasking())
        .orElse(List.of())
        .stream()
        .map(property -> {
          Preconditions.checkNotNull(property.getType(), "masking type not specified");
          Preconditions.checkArgument(
              StringUtils.isNotEmpty(property.getTopicKeysPattern())
                  || StringUtils.isNotEmpty(property.getTopicValuesPattern()),
              "topicKeysPattern or topicValuesPattern (or both) should be set for masking policy");
          return new Mask(
              Optional.ofNullable(property.getTopicKeysPattern()).map(Pattern::compile).orElse(null),
              Optional.ofNullable(property.getTopicValuesPattern()).map(Pattern::compile).orElse(null),
              MaskingPolicy.create(property)
          );
        })
        .toList();

    return new DataMasking(masks, clusterConfig.isMaskByDefault());
  }

  @VisibleForTesting
  DataMasking(List<Mask> masks, boolean maskByDefault) {
    this.masks = masks;
    this.maskByDefault = maskByDefault;
  }

  public UnaryOperator<TopicMessageDTO> getMaskerForTopic(String topic) {
    var keyMasker = getMaskingFunction(topic, Serde.Target.KEY);
    var valMasker = getMaskingFunction(topic, Serde.Target.VALUE);
    return msg -> msg
        .key(keyMasker.apply(msg.getKey()))
        .content(valMasker.apply(msg.getContent()));
  }

  @VisibleForTesting
  UnaryOperator<String> getMaskingFunction(String topic, Serde.Target target) {
    var targetMasks = masks.stream()
        .filter(m -> m.shouldBeApplied(topic, target))
        .toList();
    log.info("Disable all masking config is {}", maskByDefault);
    // If the content is a key, or disableallmasking is enabled
    if (targetMasks.isEmpty() && target == Serde.Target.KEY || !maskByDefault) {
      return UnaryOperator.identity();
    } else if (targetMasks.isEmpty() && target == Serde.Target.VALUE) {
      return s -> "\"ANONYMIZED\"";
    }
    return inputStr -> {
      if (inputStr == null) {
        return null;
      }
      try {
        JsonNode json = JSON_MAPPER.readTree(inputStr);
        if (json.isContainerNode() && json.isObject()) {
          ObjectNode original = (ObjectNode) json;
          ObjectNode temp = original.deepCopy();

          Set<String> maskedFields = new java.util.HashSet<>();

          // Apply masking rules and track affected fields
          for (Mask targetMask : targetMasks) {
            temp = (ObjectNode) targetMask.policy.applyToJsonContainer(temp, maskedFields);
          }
          final ObjectNode masked = temp;

          ObjectNode result = JSON_MAPPER.createObjectNode();
          original.fieldNames().forEachRemaining(field -> {
            log.info("Target looks like {}", target);
            if (maskedFields.contains(field) && masked.has(field)) {
              result.set(field, masked.get(field));
            } else if (target == Serde.Target.VALUE) {
              result.put(field, "ANONYMIZED");
            } else {
              result.set(field, original.get(field));
            }
          });

          return result.toString();
        }
      } catch (JsonProcessingException ignored) {
        // fallback to string-based masking
      }
      return "\"ANONYMIZED\"";
    };
  }
}
