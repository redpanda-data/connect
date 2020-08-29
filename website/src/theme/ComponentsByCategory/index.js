import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import ComponentCard from '@theme/ComponentCard';

function ComponentsByCategory(props) {
  let {type, summaries} = props;
  const context = useDocusaurusContext();
  const types = context.siteConfig.customFields.components[type];

  if ( !Array.isArray(summaries) ) {
    categoryList = [];
  }

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