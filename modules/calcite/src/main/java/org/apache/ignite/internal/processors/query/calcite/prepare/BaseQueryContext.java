/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import com.google.common.collect.Multimap;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.UnboundMetadata;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteRexBuilder;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;

/**
 * Base query context.
 */
public final class BaseQueryContext extends AbstractQueryContext {
    /** */
    private static final IgniteCostFactory COST_FACTORY = new IgniteCostFactory();

    /** */
    public static final CalciteConnectionConfig CALCITE_CONNECTION_CONFIG;

    /** */
    private static final BaseQueryContext EMPTY_CONTEXT;

    /** */
    private static final VolcanoPlanner EMPTY_PLANNER;

    /** */
    private static final RexBuilder REX_BUILDER;

    /** */
    public static final RelOptCluster CLUSTER;

    /** */
    public static final IgniteTypeFactory TYPE_FACTORY;

    static {
        Properties props = new Properties();

        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(FRAMEWORK_CONFIG.getParserConfig().caseSensitive()));
        props.setProperty(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
            String.valueOf(true));

        CALCITE_CONNECTION_CONFIG = new CalciteConnectionConfigImpl(props) {
            /**
             * Might be set by property {@link CalciteConnectionProperty#CONFORMANCE}.
             * But will try to cast interface {@link SqlConformance} to enum {@link SqlConformanceEnum}.
             */
            @Override public SqlConformance conformance() {
                return FRAMEWORK_CONFIG.getParserConfig().conformance();
            }
        };

        EMPTY_CONTEXT = builder().build();

        EMPTY_PLANNER = new VolcanoPlanner(COST_FACTORY, EMPTY_CONTEXT) {
            @Override public void registerSchema(RelOptSchema schema) {
                // This method in VolcanoPlanner stores schema in hash map. It can be invoked during relational
                // operators cloning, so, can be executed even with empty context. Override it for empty context to
                // prevent memory leaks.
            }
        };

        RelDataTypeSystem typeSys = CALCITE_CONNECTION_CONFIG.typeSystem(RelDataTypeSystem.class, FRAMEWORK_CONFIG.getTypeSystem());
        TYPE_FACTORY = new IgniteTypeFactory(typeSys);

        REX_BUILDER = new IgniteRexBuilder(TYPE_FACTORY);

        CLUSTER = RelOptCluster.create(EMPTY_PLANNER, REX_BUILDER);

        // Forbid using the empty cluster in any planning or mapping procedures to prevent memory leaks.
        String cantBeUsedMsg = "Empty cluster can't be used for planning or mapping";

        CLUSTER.setMetadataProvider(
            new RelMetadataProvider() {
                @Override public <M extends Metadata> UnboundMetadata<M> apply(
                    Class<? extends RelNode> relCls,
                    Class<? extends M> metadataCls) {
                    throw new AssertionError(cantBeUsedMsg);
                }

                @Override public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers(MetadataDef<M> def) {
                    throw new AssertionError(cantBeUsedMsg);
                }

                @Override public List<MetadataHandler<?>> handlers(Class<? extends MetadataHandler<?>> hndCls) {
                    throw new AssertionError(cantBeUsedMsg);
                }
            }
        );

        CLUSTER.setMetadataQuerySupplier(() -> {
            throw new AssertionError(cantBeUsedMsg);
        });
    }

    /** */
    private final FrameworkConfig cfg;

    /** */
    private final IgniteLogger log;

    /** */
    private final IgniteTypeFactory typeFactory;

    /** */
    private final RexBuilder rexBuilder;

    /** */
    private CalciteCatalogReader catalogReader;

    /** */
    private SqlOperatorTable opTable;

    /** */
    private final GridQueryCancel qryCancel;

    /** */
    private final boolean isLocal;

    /** */
    private final boolean forcedJoinOrder;

    /** */
    private final int[] parts;

    /**
     * Private constructor, used by a builder.
     */
    private BaseQueryContext(
        FrameworkConfig cfg,
        Context parentCtx,
        IgniteLogger log,
        boolean isLocal,
        boolean forcedJoinOrder,
        int[] parts
    ) {
        super(Contexts.chain(parentCtx, cfg.getContext()));

        // link frameworkConfig#context() to this.
        this.cfg = Frameworks.newConfigBuilder(cfg).context(this).build();

        this.log = log;

        this.isLocal = isLocal;

        this.forcedJoinOrder = forcedJoinOrder;

        this.parts = parts;

        qryCancel = unwrap(GridQueryCancel.class);

        typeFactory = TYPE_FACTORY;

        rexBuilder = REX_BUILDER;
    }

    /**
     * @return Framework config.
     */
    public FrameworkConfig config() {
        return cfg;
    }

    /**
     * @return Logger.
     */
    public IgniteLogger logger() {
        return log;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schema().getName();
    }

    /**
     * @return Schema.
     */
    public SchemaPlus schema() {
        return cfg.getDefaultSchema();
    }

    /**
     * @return Type factory.
     */
    public IgniteTypeFactory typeFactory() {
        return typeFactory;
    }

    /** */
    public RexBuilder rexBuilder() {
        return rexBuilder;
    }

    /**
     * @return Sql operators table.
     */
    public SqlOperatorTable opTable() {
        if (opTable == null)
            opTable = SqlOperatorTables.chain(config().getOperatorTable(), catalogReader());

        return opTable;
    }

    /**
     * @return New catalog reader.
     */
    public CalciteCatalogReader catalogReader() {
        if (catalogReader != null)
            return catalogReader;

        SchemaPlus dfltSchema = schema(), rootSchema = dfltSchema;

        while (rootSchema.getParentSchema() != null)
            rootSchema = rootSchema.getParentSchema();

        return catalogReader = new CalciteCatalogReader(
            CalciteSchema.from(rootSchema),
            CalciteSchema.from(dfltSchema).path(null),
            typeFactory(), CALCITE_CONNECTION_CONFIG);
    }

    /**
     * @return Query cancel.
     */
    public GridQueryCancel queryCancel() {
        return qryCancel;
    }

    /**
     * @return Context builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** */
    public static BaseQueryContext empty() {
        return EMPTY_CONTEXT;
    }

    /** */
    public boolean isLocal() {
        return isLocal;
    }

    /** */
    public boolean isForcedJoinOrder() {
        return forcedJoinOrder;
    }

    /** */
    public int[] partitions() {
        if (parts != null)
            return Arrays.copyOf(parts, parts.length);

        return null;
    }

    /**
     * Query context builder.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Builder {
        /** */
        private static final FrameworkConfig EMPTY_CONFIG =
            Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(createRootSchema(false))
                .build();

        /** */
        private FrameworkConfig frameworkCfg = EMPTY_CONFIG;

        /** */
        private Context parentCtx = Contexts.empty();

        /** */
        private IgniteLogger log = new NullLogger();

        /** */
        private boolean isLocal;

        /** */
        private boolean forcedJoinOrder;

        /** */
        private int[] parts = null;

        /**
         * @param frameworkCfg Framework config.
         * @return Builder for chaining.
         */
        public Builder frameworkConfig(@Nullable FrameworkConfig frameworkCfg) {
            if (frameworkCfg == null)
                return this;

            if (this.frameworkCfg != EMPTY_CONFIG) {
                // Schema was set explicitely earlier.
                SchemaPlus schema = this.frameworkCfg.getDefaultSchema();

                this.frameworkCfg = newConfigBuilder(frameworkCfg).defaultSchema(schema).build();
            }
            else
                this.frameworkCfg = frameworkCfg;

            return this;
        }

        /**
         * @param schema Default schema.
         * @return Builder for chaining.
         */
        public Builder defaultSchema(SchemaPlus schema) {
            frameworkCfg = newConfigBuilder(frameworkCfg)
                .defaultSchema(schema)
                .build();

            return this;
        }

        /**
         * @param parentCtx Parent context.
         * @return Builder for chaining.
         */
        public Builder parentContext(@NotNull Context parentCtx) {
            this.parentCtx = parentCtx;
            return this;
        }

        /**
         * @param log Logger.
         * @return Builder for chaining.
         */
        public Builder logger(@NotNull IgniteLogger log) {
            this.log = log;
            return this;
        }

        /**
         * @param isLocal Local execution flag.
         * @return Builder for chaining.
         */
        public Builder local(boolean isLocal) {
            this.isLocal = isLocal;
            return this;
        }

        /**
         * @param forcedJoinOrder Forced join orders flag.
         * @return Builder for chaining.
         */
        public Builder forcedJoinOrder(boolean forcedJoinOrder) {
            this.forcedJoinOrder = forcedJoinOrder;
            return this;
        }

        /**
         * @param parts Array of partitions' numbers.
         * @return Builder for chaining.
         */
        public Builder partitions(int[] parts) {
            if (parts != null)
                this.parts = Arrays.copyOf(parts, parts.length);

            return this;
        }

        /**
         * Builds planner context.
         *
         * @return Planner context.
         */
        public BaseQueryContext build() {
            return new BaseQueryContext(frameworkCfg, parentCtx, log, isLocal, forcedJoinOrder, parts);
        }
    }
}
