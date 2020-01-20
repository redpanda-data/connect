/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from 'react';
import Layout from '@theme/Layout';

function NotFound() {
  return (
    <Layout title="Page Not Found">
      <div className="container margin-vert--xl">
        <div className="row">
          <div className="col col--6 col--offset-3">
            <img src="/img/Blobsherlock.svg" className="margin-bottom--lg"/>
            <h1 className="hero__title">Woops! Page Not Found</h1>
            <p>The documentation site has recently moved, chances are that the page you're looking for is <a href="/docs/about">in the new docs section</a>.</p>
          </div>
        </div>
      </div>
    </Layout>
  );
}

export default NotFound;