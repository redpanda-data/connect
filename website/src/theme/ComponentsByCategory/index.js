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
      name: "Azure",
      description: "Inputs that consume from Microsoft Azure services.",
    },
    {
      name: "Social",
      description: "Inputs that consume from social applications and services.",
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
  buffers: [
    {
      name: "Windowing",
      description: "Buffers that provide message windowing capabilities.",
    },
    {
      name: "Utility",
      description: "Buffers that are intended for niche but general use.",
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
      name: "Social",
      description: "Outputs that write to social applications and services.",
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
    categories[summaries[i].name.toLowerCase()] = {
      summary: summaries[i].description,
      items: [],
    }
  }

  for (let i = 0; i < types.length; i++) {
    let cats = types[i].categories;
    if ( Array.isArray(cats) ) {
      for (let j = 0; j < cats.length; j++) {
        let catLower = cats[j].toLowerCase();
        if ( categories[catLower] === undefined ) {
          categoryList.push(catLower.charAt(0).toUpperCase() + catLower.slice(1));
          categories[catLower] = {
            summary: "",
            items: [types[i]],
          };
        } else {
          categories[catLower].items.push(types[i]);
        }
      }
    }
  }

  return (
    <Tabs defaultValue={categoryList[0].toLowerCase()} values={categoryList.map((cat) => (
      { label: cat, value: cat.toLowerCase() }
    ))}>
      {categoryList.map((cat) => (
        <TabItem key={cat.toLowerCase()} value={cat.toLowerCase()}>
          <p>{categories[cat.toLowerCase()].summary}</p>
          {categories[cat.toLowerCase()].items.map((data, idx) => (
            <ComponentCard key={idx} type={type} component={data} />
          ))}
        </TabItem>
      ))}
    </Tabs>
  );
}

export default ComponentsByCategory;