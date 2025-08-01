{% macro build_knowledge_graph_edges(
    source_table,
    text_column,
    document_id_column,
    chunk_size=1000,
    overlap_size=200,
    entity_types=['PERSON', 'ORGANIZATION', 'LOCATION', 'CONCEPT'],
    confidence_threshold=0.7
) %}

  {%- set entity_types_str = entity_types | join("','") -%}

  WITH document_chunks AS (
    -- Split documents into overlapping chunks for better entity extraction
    SELECT 
      {{ document_id_column }} as document_id,
      ROW_NUMBER() OVER (PARTITION BY {{ document_id_column }} ORDER BY chunk_start) as chunk_id,
      chunk_start,
      chunk_end,
      chunk_text,
      chunk_vector_embedding
    FROM (
      SELECT 
        {{ document_id_column }},
        generate_series(1, LENGTH({{ text_column }}), {{ chunk_size - overlap_size }}) as chunk_start,
        generate_series(1, LENGTH({{ text_column }}), {{ chunk_size - overlap_size }}) + {{ chunk_size }} as chunk_end,
        SUBSTRING({{ text_column }}, 
                 generate_series(1, LENGTH({{ text_column }}), {{ chunk_size - overlap_size }}), 
                 {{ chunk_size }}) as chunk_text,
        -- Placeholder for vector embedding - would integrate with embedding service
        NULL as chunk_vector_embedding
      FROM {{ source_table }}
      WHERE {{ text_column }} IS NOT NULL 
        AND LENGTH({{ text_column }}) > 0
    ) chunked
  ),

  extracted_entities AS (
    -- Extract named entities from chunks
    -- In production, this would call an NER service or use ML models
    SELECT 
      document_id,
      chunk_id,
      entity_text,
      entity_type,
      entity_confidence,
      start_position,
      end_position,
      -- Generate deterministic entity IDs for deduplication
      {{ dbt_utils.generate_surrogate_key(['LOWER(TRIM(entity_text))', 'entity_type']) }} as entity_id
    FROM document_chunks
    CROSS JOIN LATERAL (
      -- Simulated NER extraction - in reality would use Python UDF or external service
      SELECT 
        unnest(regexp_split_to_array(chunk_text, '[.!?]+')) as sentence,
        generate_series(1, 10) as entity_seq
    ) sentences
    CROSS JOIN LATERAL (
      SELECT 
        CASE 
          WHEN entity_seq <= 3 THEN SPLIT_PART(sentence, ' ', entity_seq)
          ELSE NULL 
        END as entity_text,
        CASE 
          WHEN entity_seq = 1 THEN 'PERSON'
          WHEN entity_seq = 2 THEN 'ORGANIZATION' 
          WHEN entity_seq = 3 THEN 'CONCEPT'
          ELSE 'UNKNOWN'
        END as entity_type,
        RANDOM() * 0.3 + 0.7 as entity_confidence, -- Simulated confidence
        POSITION(sentence IN chunk_text) as start_position,
        POSITION(sentence IN chunk_text) + LENGTH(sentence) as end_position
    ) entity_extraction
    WHERE entity_text IS NOT NULL 
      AND LENGTH(TRIM(entity_text)) > 2
      AND entity_type IN ('{{ entity_types_str }}')
      AND entity_confidence >= {{ confidence_threshold }}
  ),

  entity_nodes AS (
    -- Deduplicate and create entity nodes
    SELECT DISTINCT
      entity_id,
      LOWER(TRIM(entity_text)) as canonical_name,
      entity_type,
      AVG(entity_confidence) as avg_confidence,
      COUNT(*) as mention_count,
      ARRAY_AGG(DISTINCT document_id) as source_documents,
      -- Entity properties for graph enrichment
      CASE 
        WHEN entity_type = 'PERSON' THEN 'blue'
        WHEN entity_type = 'ORGANIZATION' THEN 'red'
        WHEN entity_type = 'LOCATION' THEN 'green'
        WHEN entity_type = 'CONCEPT' THEN 'purple'
        ELSE 'gray'
      END as node_color,
      CURRENT_TIMESTAMP as created_at,
      CURRENT_TIMESTAMP as updated_at
    FROM extracted_entities
    GROUP BY entity_id, entity_text, entity_type
    HAVING COUNT(*) >= 2  -- Only keep entities mentioned multiple times
  ),

  entity_cooccurrences AS (
    -- Find entities that appear in the same chunks (relationship candidates)
    SELECT 
      e1.entity_id as source_entity_id,
      e2.entity_id as target_entity_id,
      e1.document_id,
      e1.chunk_id,
      -- Calculate relationship strength based on proximity and co-occurrence
      CASE 
        WHEN ABS(e1.start_position - e2.start_position) < 100 THEN 'STRONG'
        WHEN ABS(e1.start_position - e2.start_position) < 300 THEN 'MEDIUM'
        ELSE 'WEAK'
      END as relationship_strength,
      ABS(e1.start_position - e2.start_position) as distance,
      (e1.entity_confidence + e2.entity_confidence) / 2 as combined_confidence
    FROM extracted_entities e1
    JOIN extracted_entities e2 
      ON e1.document_id = e2.document_id 
      AND e1.chunk_id = e2.chunk_id
      AND e1.entity_id != e2.entity_id
    WHERE ABS(e1.start_position - e2.start_position) < 1000  -- Within reasonable proximity
  ),

  relationship_edges AS (
    -- Aggregate co-occurrences into weighted relationships
    SELECT 
      {{ dbt_utils.generate_surrogate_key(['source_entity_id', 'target_entity_id']) }} as edge_id,
      source_entity_id,
      target_entity_id,
      -- Determine relationship type based on entity types
      CASE 
        WHEN s.entity_type = 'PERSON' AND t.entity_type = 'ORGANIZATION' THEN 'WORKS_FOR'
        WHEN s.entity_type = 'PERSON' AND t.entity_type = 'LOCATION' THEN 'LOCATED_IN'
        WHEN s.entity_type = 'ORGANIZATION' AND t.entity_type = 'LOCATION' THEN 'BASED_IN'
        WHEN s.entity_type = t.entity_type THEN 'RELATED_TO'
        ELSE 'MENTIONS'
      END as relationship_type,
      COUNT(*) as co_occurrence_count,
      AVG(combined_confidence) as avg_confidence,
      MIN(distance) as min_distance,
      AVG(distance) as avg_distance,
      -- Calculate edge weight for graph algorithms
      LN(COUNT(*) + 1) * AVG(combined_confidence) * 
        (1.0 / (1.0 + AVG(distance) / 1000.0)) as edge_weight,
      ARRAY_AGG(DISTINCT document_id) as supporting_documents,
      COUNT(DISTINCT document_id) as document_support,
      CURRENT_TIMESTAMP as created_at,
      CURRENT_TIMESTAMP as updated_at
    FROM entity_cooccurrences co
    JOIN entity_nodes s ON co.source_entity_id = s.entity_id
    JOIN entity_nodes t ON co.target_entity_id = t.entity_id
    GROUP BY source_entity_id, target_entity_id, s.entity_type, t.entity_type
    HAVING COUNT(*) >= 2  -- Only keep relationships with multiple supporting instances
      AND AVG(combined_confidence) >= {{ confidence_threshold }}
  )

  -- Final output: combine nodes and edges for knowledge graph
  SELECT 
    'node' as graph_element_type,
    entity_id as element_id,
    canonical_name as element_name,
    entity_type as element_type,
    NULL as source_id,
    NULL as target_id,
    avg_confidence as confidence,
    mention_count as weight,
    JSON_BUILD_OBJECT(
      'node_color', node_color,
      'mention_count', mention_count,
      'source_documents', source_documents,
      'avg_confidence', avg_confidence
    ) as properties,
    created_at,
    updated_at
  FROM entity_nodes

  UNION ALL

  SELECT 
    'edge' as graph_element_type,
    edge_id as element_id,
    relationship_type as element_name,
    relationship_type as element_type,
    source_entity_id as source_id,
    target_entity_id as target_id,
    avg_confidence as confidence,
    edge_weight as weight,
    JSON_BUILD_OBJECT(
      'co_occurrence_count', co_occurrence_count,
      'min_distance', min_distance,
      'avg_distance', avg_distance,
      'supporting_documents', supporting_documents,
      'document_support', document_support
    ) as properties,
    created_at,
    updated_at
  FROM relationship_edges
  
  ORDER BY graph_element_type, weight DESC

{% endmacro %}


-- Example usage macro for document processing
{% macro process_documents(source_docs_table) %}
  
  {{ config(
      materialized='incremental',
      unique_key='element_id',
      on_schema_change='sync_all_columns',
      indexes=[
        {'columns': ['graph_element_type', 'element_type'], 'type': 'btree'},
        {'columns': ['source_id', 'target_id'], 'type': 'btree'},
        {'columns': ['weight'], 'type': 'btree'},
        {'columns': ['confidence'], 'type': 'btree'}
      ]
  ) }}

  {{
    build_knowledge_graph_edges(
      source_table=source_docs_table,
      text_column='content',
      document_id_column='document_id',
      chunk_size=1500,
      overlap_size=300,
      entity_types=['PERSON', 'ORGANIZATION', 'LOCATION', 'PRODUCT', 'CONCEPT'],
      confidence_threshold=0.75
    )
  }}

{% endmacro %}
