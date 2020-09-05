import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import ComponentCard from '@theme/ComponentCard';

let descriptions = {
  inputs: [
    {
      name: "Services",
      description: "Inputs that consume from storage or message streaming services.",
    },
    {
      name: "Network",
      description: "Inputs that consume directly from low level network protocols.",
    },
    {
      name: "AWS",
      description: "Inputs that consume from Amazon Web Services products.",
    },
    {
      name: "GCP",
      description: "Inputs that consume from Google Cloud Platform services.",
    },
    {
      name: "Local",
      description: "Inputs that consume from the local machine/filesystem.",
    },
    {
      name: "Utility",
      description: "Inputs that provide utility by generating data or combining/wrapping other inputs.",
    },
  ],
  processors: [
    {
      name: "Mapping",
      description: "Processors that specialize in restructuring messages.",
    },
    {
      name: "Integration",
      description: "Processors that interact with external services.",
    },
    {
      name: "Parsing",
      description: "Processors that specialize in translating messages from one format to another.",
    },
    {
      name: "Composition",
      description: "Higher level processors that compose other processors and modify their behavior.",
    },
    {
      name: "Utility",
      description: "Processors that provide general utility or do not fit in another category.",
    },
  ],
  outputs: [
    {
      name: "Services",
      description: "Outputs that write to storage or message streaming services.",
    },
    {
      name: "Network",
      description: "Outputs that write directly to low level network protocols.",
    },
    {
      name: "AWS",
      description: "Outputs that write to Amazon Web Services products.",
    },
    {
      name: "GCP",
      description: "Outputs that write to Google Cloud Platform services.",
    },
    {
      name: "Azure",
      description: "Outputs that write to Microsoft Azure services.",
    },
    {
      name: "Local",
      description: "Outputs that write to the local machine/filesystem.",
    },
    {
      name: "Utility",
      description: "Outputs that provide utility by combining/wrapping other outputs.",
    },
  ],
};

function ComponentsByCategory(props) {
  let {type} = props;
  const context = useDocusaurusContext();
  const types = context.siteConfig.customFields.components[type];

  let summaries = descriptions[type] || [];

  let categories = {};
  let categoryList = [];
  for (let i = 0; i < summaries.length; i++) {
    categoryList.push(summaries[i].name);
    categories[summaries[i].name] = {
      summary: summaries[i].description,
      items: [],
    }
  }

  for (let i = 0; i < types.length; i++) {
    let cats = types[i].categories;
    if ( Array.isArray(cats) ) {
      for (let j = 0; j < cats.length; j++) {
        if ( categories[cats[j]] === undefined ) {
          categoryList.push(cats[j]);
          categories[cats[j]] = {
            summary: "",
            items: [types[i]],
          };
        } else {
          categories[cats[j]].items.push(types[i]);
        }
      }
    }
  }

  return (
    <Tabs defaultValue={categoryList[0].toLowerCase()} values={categoryList.map((cat) => (
      { label: cat, value: cat.toLowerCase() }
    ))}>
      {categoryList.map((cat) => (
        <TabItem value={cat.toLowerCase()}>
          <p>{categories[cat].summary}</p>
          {categories[cat].items.map((data) => (
            <ComponentCard type={type} component={data} />
          ))}
        </TabItem>
      ))}
    </Tabs>
  );
}

export default ComponentsByCategory;