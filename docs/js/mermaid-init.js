document$.subscribe(() => {
  if (typeof mermaid === "undefined") {
    return;
  }

  mermaid.initialize({
    startOnLoad: false,
    securityLevel: "loose",
    theme: "base",
    fontFamily: "var(--md-mermaid-font-family)",
    themeVariables: {
      fontFamily: "var(--md-mermaid-font-family)",
      primaryColor: "var(--md-mermaid-node-bg-color)",
      primaryBorderColor: "var(--md-mermaid-node-fg-color)",
      primaryTextColor: "var(--md-mermaid-label-fg-color)",
      lineColor: "var(--md-mermaid-edge-color)",
      secondaryColor: "var(--md-mermaid-node-bg-color)",
      tertiaryColor: "var(--md-mermaid-node-bg-color)",
      clusterBkg: "var(--md-mermaid-node-bg-color)",
      clusterBorder: "var(--md-mermaid-node-fg-color)",
      edgeLabelBackground: "var(--md-mermaid-label-bg-color)",
      textColor: "var(--md-mermaid-label-fg-color)",
      mainBkg: "var(--md-mermaid-label-bg-color)",
    },
    themeCSS: `
      .node rect,
      .node circle,
      .node ellipse,
      .node polygon,
      .node path {
        fill: var(--md-mermaid-node-bg-color) !important;
        stroke: var(--md-mermaid-node-fg-color) !important;
        stroke-width: 1px !important;
      }

      .label,
      .label text,
      .nodeLabel,
      .edgeLabel,
      .edgeLabel p,
      .cluster-label,
      .cluster-label text {
        fill: var(--md-mermaid-label-fg-color) !important;
        color: var(--md-mermaid-label-fg-color) !important;
      }

      .edgePath path,
      .flowchart-link,
      .arrowheadPath,
      .marker path {
        stroke: var(--md-mermaid-edge-color) !important;
        fill: var(--md-mermaid-edge-color) !important;
      }

      .edgeLabel rect,
      .labelBkg {
        fill: var(--md-mermaid-label-bg-color) !important;
      }

      .cluster rect {
        fill: var(--md-mermaid-node-bg-color) !important;
        stroke: var(--md-mermaid-node-fg-color) !important;
      }
    `,
  });

  mermaid.run({
    querySelector: ".mermaid",
  });
});
