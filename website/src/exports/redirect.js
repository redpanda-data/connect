import React from 'react';
import {Redirect} from '@docusaurus/router';

function To(props) {
  let {location} = props;
  let {to, forComponent} = props.dest;

  if ( forComponent ) {
    if ( location.hash && location.hash.length > 1 ) {
      to = to + location.hash.slice(1);
    } else {
      to = to + '/about';
    }
  }

  return <Redirect to={to} />
}

export default To;
