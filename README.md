# MRKGEC
Multi-model Recurrent Knowledge Graph Embedding for Context-aware recommendation system.


Recommenders can be improved by exploiting the huge disposal of multi-context data that is now available. Knowledge Graphs (KGs) offer an intuitive way for incorporating this kind of assorted data.  In this paper, we introduce a context-aware recommender based on deriving graph embeddings by learning the representations of appropriate meta-paths mined from a graph database. Our system uses a number of LSTMs to model the meta-path semantics between a user-item pair, based on the length of the mined path, a Multi-head Attention module as an attention mechanism, along with a pooling and a recommendation layer. Our evaluation shows that our system is on par with state-of-the-art recommenders, while also supporting contextual modeling.

In this repository you can find:
1) The pre-processing script that created the csv files for the Neo4j uploads.
2) The Neo4j terminal import command.
3) The python scripts containing the Neo4j drivers queries.
4) The AI model.
