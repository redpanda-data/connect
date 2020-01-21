import React from 'react';
import {Redirect} from '@docusaurus/router';

const path = require('path');

function To(props) {
  let {location} = props;
  let {to, forComponent} = props.dest;

  if ( forComponent ) {
    if ( location.hash && location.hash.length > 1 ) {
      to = path.join(to, location.hash.slice(1));
    } else {
      to = path.join(to, 'about');
    }
  }

  return <Redirect to={to} />
}

export default To;