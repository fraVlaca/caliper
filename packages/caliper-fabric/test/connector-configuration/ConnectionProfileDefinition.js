/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

'use strict';

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
chai.should();
const path = require('path');
const fs = require('fs');

const ConnectionProfileDefinition = require('../../lib/connector-configuration/ConnectionProfileDefinition');
const CaliperUtils = require('@hyperledger/caliper-core/lib/common/utils/caliper-utils');

describe('A Connection Profile Definition', async () => {
    const connectionProfile = fs.readFileSync(path.resolve(__dirname, '../sample-configs/Org1ConnectionProfile.json'));
    const staticConnectionProfile = fs.readFileSync(path.resolve(__dirname, '../sample-configs/StaticOrg1ConnectionProfile.json'));
    const noPeerConnectionProfile = {
        peers: {
        }
    };
    const blankConnectionProfile = {};
    const mspId = 'Org1MSP';

    it('should return whether it is dynamic or not (true or false) based on the discover property', () => {
        const providedConnectionPofile = JSON.parse(connectionProfile.toString());
        let  connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
            loadedConnectionProfile: providedConnectionPofile,
            discover: true
        });
        connectionProfileDefinition.getConnectionProfile().should.equal(providedConnectionPofile);
        connectionProfileDefinition.isDynamicConnectionProfile().should.equal(true);

        connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
            loadedConnectionProfile: providedConnectionPofile,
            discover: false
        });
        connectionProfileDefinition.isDynamicConnectionProfile().should.equal(false);

        connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
            loadedConnectionProfile: providedConnectionPofile
        });
        connectionProfileDefinition.isDynamicConnectionProfile().should.equal(false);
    });


    it('should return true if a connection profile is using tls', () => {
        const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
            loadedConnectionProfile: JSON.parse(connectionProfile.toString()),
            discover: true
        });
        connectionProfileDefinition.isTLSEnabled().should.equal(true);
    });

    it('should return false if a connection profile is not using tls', () => {
        const alteredConnectionProfile = JSON.parse(connectionProfile.toString());
        alteredConnectionProfile.peers['peer0.org1.example.com'].url = 'grpc://localhost:7051';
        alteredConnectionProfile.certificateAuthorities['ca.org1.example.com'].url = 'http://localhost:7054';
        const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
            loadedConnectionProfile: alteredConnectionProfile,
            discover: true
        });
        connectionProfileDefinition.isTLSEnabled().should.equal(false);
    });

    describe('when getting the organization owned endorsing peers for a channel', () => {
        it('should get the owned peers for a channel when defined in the connection profile', () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: JSON.parse(staticConnectionProfile.toString()),
                discover: false
            });

            let peersInChannelForOrg = connectionProfileDefinition.getOwnedEndorsingPeersInChannel('mychannel');
            peersInChannelForOrg.should.deep.equal(['peer0.org1.example.com', 'peer1.org1.example.com']);

            peersInChannelForOrg = connectionProfileDefinition.getOwnedEndorsingPeersInChannel('yourchannel');
            peersInChannelForOrg.should.deep.equal(['peer0.org1.example.com']);
        });

        it('should throw an error when no channels are defined in the connection profile', () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: blankConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getOwnedEndorsingPeersInChannel('mychannel');
            }).should.throw(/No channel mychannel defined in the connection profile for organization Org1MSP/);
        });

        it('should throw an error when the specific channel is not defined in the connection profile', () => {
            const limitedConnectionProfile = {
                channels: {
                    yourchannel: {
                        orderers: [
                            'orderer0.example.com',
                            'orderer1.example.com'
                        ],
                        peers: {
                            'peer0.org1.example.com': {},
                            'peer0.org2.example.com': {}
                        }
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: limitedConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getOwnedEndorsingPeersInChannel('mychannel');
            }).should.throw(/No channel mychannel defined in the connection profile for organization Org1MSP/);
        });

        it('should throw an error when the specific channel has no peers in the connection profile', () => {
            const limitedConnectionProfile = {
                channels: {
                    mychannel: {
                        orderers: [
                            'orderer0.example.com',
                            'orderer1.example.com'
                        ]
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: limitedConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getOwnedEndorsingPeersInChannel('mychannel');
            }).should.throw(/No peers defined for mychannel in the connection profile for organization Org1MSP/);
        });

        it('should return an empty list when no peers are specified for the organization', () => {
            const limitedConnectionProfile = {
                client: {
                    organization: 'Org1'
                },
                organizations: {
                    Org1: {
                        mspid: 'Org1MSP'
                    }
                },
                channels: {
                    mychannel: {
                        orderers: [
                            'orderer0.example.com',
                            'orderer1.example.com'
                        ],
                        peers: {
                            'peer0.org1.example.com': {},
                            'peer0.org2.example.com': {}
                        }
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: limitedConnectionProfile,
                discover: false
            });
            connectionProfileDefinition.getOwnedEndorsingPeersInChannel('mychannel').should.deep.equal([]);
        });

        it('should return only peers that are defined as endorsing or chaincode query peers', () => {
            const limitedConnectionProfile = {
                client: {
                    organization: 'Org1'
                },
                organizations: {
                    Org1: {
                        mspid: 'Org1MSP',
                        peers: [
                            'peer0.org1.example.com',
                            'peer1.org1.example.com',
                            'peer2.org1.example.com',
                            'peer3.org1.example.com',
                            'peer4.org1.example.com',
                            'peer5.org1.example.com'
                        ]
                    }
                },
                channels: {
                    mychannel: {
                        orderers: [
                            'orderer0.example.com',
                            'orderer1.example.com'
                        ],
                        peers: {
                            'peer0.org1.example.com': {endorsingPeer: false, chaincodeQuery:false},
                            'peer1.org1.example.com': {endorsingPeer: true, chaincodeQuery: false},
                            'peer2.org1.example.com': {endorsingPeer: false, chaincodeQuery: true},
                            'peer3.org1.example.com': {endorsingPeer: false},
                            'peer4.org1.example.com': {endorsingPeer: false, chaincodeQuery:false},
                            'peer5.org1.example.com': {chaincodeQuery: false},
                            'peer0.org2.example.com': {}
                        }
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: limitedConnectionProfile,
                discover: false
            });
            connectionProfileDefinition.getOwnedEndorsingPeersInChannel('mychannel').should.deep.equal([
                'peer1.org1.example.com',
                'peer2.org1.example.com',
                'peer3.org1.example.com',
                'peer5.org1.example.com'
            ]);
        });
    });

    describe('when getting the endorsing peers for a channel', () => {
        it('should get the owned peers for a channel when defined in the connection profile', () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: JSON.parse(staticConnectionProfile.toString()),
                discover: false
            });

            let peersInChannelForOrg = connectionProfileDefinition.getEndorsingPeersInChannel('mychannel');
            peersInChannelForOrg.should.deep.equal(['peer0.org1.example.com', 'peer1.org1.example.com', 'peer0.org2.example.com']);

            peersInChannelForOrg = connectionProfileDefinition.getEndorsingPeersInChannel('yourchannel');
            peersInChannelForOrg.should.deep.equal(['peer0.org1.example.com', 'peer0.org2.example.com']);
        });

        it('should throw an error when no channels are defined in the connection profile', () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: blankConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getEndorsingPeersInChannel('mychannel');
            }).should.throw(/No channel mychannel defined in the connection profile for organization Org1MSP/);
        });

        it('should throw an error when the specific channel is not defined in the connection profile', () => {
            const limitedConnectionProfile = {
                channels: {
                    yourchannel: {
                        orderers: [
                            'orderer0.example.com',
                            'orderer1.example.com'
                        ],
                        peers: {
                            'peer0.org1.example.com': {},
                            'peer0.org2.example.com': {}
                        }
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: limitedConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getEndorsingPeersInChannel('mychannel');
            }).should.throw(/No channel mychannel defined in the connection profile for organization Org1MSP/);
        });

        it('should throw an error when the specific channel has no peers in the connection profile', () => {
            const limitedConnectionProfile = {
                channels: {
                    mychannel: {
                        orderers: [
                            'orderer0.example.com',
                            'orderer1.example.com'
                        ]
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: limitedConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getEndorsingPeersInChannel('mychannel');
            }).should.throw(/No peers defined for mychannel in the connection profile for organization Org1MSP/);
        });

        it('should return only peers that are defined as endorsing or chaincode query peers', () => {
            const limitedConnectionProfile = {
                client: {
                    organization: 'Org1'
                },
                organizations: {
                    Org1: {
                        mspid: 'Org1MSP',
                        peers: [
                            'peer0.org1.example.com',
                            'peer1.org1.example.com',
                            'peer2.org1.example.com',
                            'peer3.org1.example.com',
                            'peer4.org1.example.com',
                            'peer5.org1.example.com'
                        ]
                    }
                },
                channels: {
                    mychannel: {
                        orderers: [
                            'orderer0.example.com',
                            'orderer1.example.com'
                        ],
                        peers: {
                            'peer0.org1.example.com': {endorsingPeer: false, chaincodeQuery:false},
                            'peer1.org1.example.com': {endorsingPeer: true, chaincodeQuery: false},
                            'peer2.org1.example.com': {endorsingPeer: false, chaincodeQuery: true},
                            'peer3.org1.example.com': {endorsingPeer: false},
                            'peer4.org1.example.com': {endorsingPeer: false, chaincodeQuery:false},
                            'peer5.org1.example.com': {chaincodeQuery: false},
                            'peer0.org2.example.com': {endorsingPeer: false, chaincodeQuery:false},
                            'peer1.org2.example.com': {endorsingPeer: true, chaincodeQuery: false},
                            'peer2.org2.example.com': {endorsingPeer: false, chaincodeQuery: true},
                            'peer3.org2.example.com': {endorsingPeer: false},
                            'peer4.org2.example.com': {endorsingPeer: false, chaincodeQuery:false},
                            'peer5.org2.example.com': {chaincodeQuery: false},
                        }
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: limitedConnectionProfile,
                discover: false
            });
            connectionProfileDefinition.getEndorsingPeersInChannel('mychannel').should.deep.equal([
                'peer1.org1.example.com',
                'peer2.org1.example.com',
                'peer3.org1.example.com',
                'peer5.org1.example.com',
                'peer1.org2.example.com',
                'peer2.org2.example.com',
                'peer3.org2.example.com',
                'peer5.org2.example.com'
            ]);
        });
    });

    describe('when getting the orderers for a channel', () => {
        it('should get the orderers for a channel when defined in the connection profile', () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: JSON.parse(staticConnectionProfile.toString()),
                discover: false
            });
            const orderers = connectionProfileDefinition.getOrderersForChannel('mychannel');
            orderers.should.deep.equal(['orderer0.example.com', 'orderer1.example.com']);
        });

        it('should throw an error when no channels are defined in the connection profile', () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: blankConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getOrderersForChannel('mychannel');
            }).should.throw(/No channel mychannel defined in the connection profile for organization Org1MSP/);
        });

        it('should throw an error when the specific channel is not defined in the connection profile', () => {
            const limitedConnectionProfile = {
                channels: {
                    yourchannel: {
                        orderers: [
                            'orderer0.example.com',
                            'orderer1.example.com'
                        ],
                        peers: {
                            'peer0.org1.example.com': {},
                            'peer0.org2.example.com': {}
                        }
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: limitedConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getOrderersForChannel('mychannel');
            }).should.throw(/No channel mychannel defined in the connection profile for organization Org1MSP/);
        });

        it('should throw an error when the specific channel has no orderers in the connection profile', () => {
            const limitedConnectionProfile = {
                channels: {
                    mychannel: {
                        peers: {
                            'peer0.org1.example.com': {},
                            'peer0.org2.example.com': {}
                        }
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: limitedConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getOrderersForChannel('mychannel');
            }).should.throw(/No orderers defined for mychannel in the connection profile for organization Org1MSP/);
        });

        it('should throw an error when the specific channel does not define the orderer list as an array in the connection profile', () => {
            const limitedConnectionProfile = {
                channels: {
                    mychannel: {
                        orderers: {
                            'orderer0.example.com': '',
                            'orderer1.example.com': ''
                        },
                        peers: {
                            'peer0.org1.example.com': {},
                            'peer0.org2.example.com': {}
                        }
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: limitedConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getOrderersForChannel('mychannel');
            }).should.throw(/No orderers defined for mychannel in the connection profile for organization Org1MSP/);
        });
    });

    describe('when gettin the list op peer for the organization', () => {

        it('should return the list of all peers name in the organization', () => {
            const limitedConnectionProfile = {
                client: {
                    organization: 'Org1'
                },
                organizations: {
                    Org1: {
                        mspid: 'Org1MSP',
                        peers: [
                            'peer0.org1.example.com',
                            'peer1.org1.example.com',
                            'peer2.org1.example.com',
                            'peer3.org1.example.com',
                            'peer4.org1.example.com',
                            'peer5.org1.example.com'
                        ]
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: limitedConnectionProfile,
                discover: false
            });
            connectionProfileDefinition.getPeersListForOrganization().should.deep.equal([
                'peer0.org1.example.com',
                'peer1.org1.example.com',
                'peer2.org1.example.com',
                'peer3.org1.example.com',
                'peer4.org1.example.com',
                'peer5.org1.example.com'
            ]);
        });

        it('should through an error if no client property was found', async () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: blankConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getPeersListForOrganization();
            }).should.throw(
                'No client property can be found in the connection profile provided'
            );
        });

        it('should through an error if no organisation property was found in connectionProfile.client', async () => {
            const noOrgConnectionProfile = {
                client: {}
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: noOrgConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getPeersListForOrganization();
            }).should.throw(
                'No organization property can be found in connectionProfile.client'
            );
        });

        it('should through an error if no organisations property was found in the connectionProfile provided', async () => {
            const noOrgConnectionProfile = {
                client: {
                    organization: 'Org1'
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: noOrgConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getPeersListForOrganization();
            }).should.throw(
                'No organizations property can be found for the connection profile provided'
            );
        });

        it('should through an error if the org defined in the connectionProfile.client property cannot be found in connectionProfile.organizations', async () => {
            const noOrgConnectionProfile = {
                client: {
                    organization: 'Org1'
                },
                organizations: {}
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: noOrgConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getPeersListForOrganization();
            }).should.throw(
                'Connection Profile Definition Format error: Org1 defined in the connectionProfile.client property cannot be found in connectionProfile.organizations'
            );
        });

        it('should through an error if the org1 listed in connectionProfile.organizations does not have any peers property', async () => {
            const noOrgConnectionProfile = {
                client: {
                    organization: 'Org1'
                },
                organizations: {
                    Org1:{}
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: noOrgConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getPeersListForOrganization();
            }).should.throw(
                'Org1 listed in connectionProfile.organizations does not have any peers property'
            );
        });
    });

    describe('when getting the Tls certificate for a peer', () => {

        const peer = 'peer0.org1.example.com';

        it('should get the pem if embeded in the connectioProfile under .tlsCACerts.pem', async () => {
            const dinamicConnectionProfile = JSON.parse(connectionProfile.toString());
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: dinamicConnectionProfile,
                discover: true
            });
            (await connectionProfileDefinition.getTlsCertForPeer(peer)).should.deep.equal(
                dinamicConnectionProfile.peers[peer].tlsCACerts.pem
            );
        });

        it('should through an error if path provided does not point to a certificate', async () => {
            const invalidPathConnectionProfile = {
                peers: {
                    'peer0.org1.example.com': {
                        tlsCACerts: {
                            path:'./invalid.pem'
                        }
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: invalidPathConnectionProfile,
                discover: true
            });
            await connectionProfileDefinition.getTlsCertForPeer(peer).should.be.rejectedWith(
                `path property does not point to a file that exists at /Users/vlaca/Documents/work/caliper/packages/caliper-fabric/invalid.pem for ${peer}`
            );
        });

        it('should get the pem if provided through path', async () => {
            const validPathConnectionProfile = {
                peers: {
                    'peer0.org1.example.com': {
                        tlsCACerts: {
                            path:'./test/sample-configs/User1.cert.pem'
                        }
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: validPathConnectionProfile,
                discover: false
            });
            CaliperUtils.resolvePath(validPathConnectionProfile.peers[peer].tlsCACerts.path);
            (await connectionProfileDefinition.getTlsCertForPeer(peer)).should.deep.equal(
                '-----BEGIN CERTIFICATE-----\nMIICKzCCAdGgAwIBAgIRAL0i4WmltsbdL5xDc0xJQYQwCgYIKoZIzj0EAwIwczEL\nMAkGA1UEBhMCVVMxEzARBgNVBAgTCkNhbGlmb3JuaWExFjAUBgNVBAcTDVNhbiBG\ncmFuY2lzY28xGTAXBgNVBAoTEG9yZzEuZXhhbXBsZS5jb20xHDAaBgNVBAMTE2Nh\nLm9yZzEuZXhhbXBsZS5jb20wHhcNMjAwOTA3MTE0MjAwWhcNMzAwOTA1MTE0MjAw\nWjBsMQswCQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMN\nU2FuIEZyYW5jaXNjbzEPMA0GA1UECxMGY2xpZW50MR8wHQYDVQQDDBZVc2VyMUBv\ncmcxLmV4YW1wbGUuY29tMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEZ6BjhMNZ\nPjLYxx+Mtq08UY7Tmill5xRqbACy13wZCmb8SIW6/pjzhWVWfM7YoSLGQWgrgiB4\n8NU8eubMyQA3DqNNMEswDgYDVR0PAQH/BAQDAgeAMAwGA1UdEwEB/wQCMAAwKwYD\nVR0jBCQwIoAgnvPwKjaMDSoQBDUfZMgJPmr5nlvrV/AdzLomWFMuLbkwCgYIKoZI\nzj0EAwIDSAAwRQIhAJwCKxXrCGZMgBlxbaMJzN7wcUM2qjX8jS4ZnBDl7HpaAiBH\nNhHITMTKPcPKgrQT/h1bTXqmxZXnwgh1n7D7VC/Fuw==\n-----END CERTIFICATE-----\n'
            );
        });

        it('should through an error if pem was not provide through either path or embeded in .tlsCACerts.pem', async () => {
            const noPemConnectionProfile = {
                peers: {
                    'peer0.org1.example.com': {
                        tlsCACerts: {
                        }
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: noPemConnectionProfile,
                discover: false
            });
            await connectionProfileDefinition.getTlsCertForPeer(peer).should.be.rejectedWith(
                `No valid tls cert option provided in the ${peer}.tlsCACert property of connection profile`
            );
        });

        it('should through an error if .tlsCACerts of peer was not provided in the connection Profile', async () => {
            const notlsCACertConnectionProfile = {
                peers: {
                    'peer0.org1.example.com': {
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: notlsCACertConnectionProfile,
                discover: false
            });
            await connectionProfileDefinition.getTlsCertForPeer(peer).should.be.rejectedWith(
                `No tlsCACert property of ${peer} in the connection profile was not provided`
            );
        });

        it('should through an error if peer specified was not provided in the connection Profile', async () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: noPeerConnectionProfile,
                discover: false
            });
            await connectionProfileDefinition.getTlsCertForPeer(peer).should.be.rejectedWith(
                `${peer} provided is not present in the connection profile`
            );
        });

        it('should through an error if no peers property is found', async () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: blankConnectionProfile,
                discover: false
            });
            await connectionProfileDefinition.getTlsCertForPeer(peer).should.be.rejectedWith(
                'No peers property can be found in the connection profile provided'
            );
        });
    });


    describe('when getting the Tls certificate for a peer', () => {

        const peer = 'peer0.org1.example.com';

        it('should return the parsed url and true for a grpcs url', async () => {
            const grpcsConnectionProfile = {
                peers: {
                    'peer0.org1.example.com': {
                        url: 'grpcs://localhost:7051'
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: grpcsConnectionProfile,
                discover: true
            });
            connectionProfileDefinition.getGrpcEndPointForPeer(peer).should.deep.equal(
                ('localhost:7051', true)
            );
        });

        it('should return the parsed url and false for a grpc url', async () => {
            const grpcConnectionProfile = {
                peers: {
                    'peer0.org1.example.com': {
                        url: 'grpc://localhost:7051'
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: grpcConnectionProfile,
                discover: true
            });
            connectionProfileDefinition.getGrpcEndPointForPeer(peer).should.deep.equal(
                ('localhost:7051', false)
            );
        });

        it('should through an error if url provided is not a valid grpc/grpcs url', async () => {
            const wrongUrlConnectionProfile = {
                peers: {
                    'peer0.org1.example.com': {
                        url: 'http://localhost:7051'
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: wrongUrlConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getGrpcEndPointForPeer(peer);
            }).should.throw(
                'http://localhost:7051 is not a valid grpc/grpcs url, make sure to prefix grpc:// or grpcs:// at the beginning of the url'
            );
        });


        it('should through an error if peer specified was not provided in the connection Profile', async () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: noPeerConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getGrpcEndPointForPeer(peer);
            }).should.throw(
                `${peer} provided is not present in the connection profile`
            );
        });

        it('should through an error if no peers property is found', async () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: blankConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getGrpcEndPointForPeer(peer);
            }).should.throw(
                'No peers property can be found in the connection profile provided'
            );
        });

        it('should through an error if no url property for peer is found', async () => {
            const noUrlConnectionProfile = {
                peers: {
                    'peer0.org1.example.com': {
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: noUrlConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getGrpcEndPointForPeer(peer);
            }).should.throw(
                `${peer} provided does not have url property provided in the connection Profile`            );
        });
    });

    describe('when getting the grpc options for a peer', () => {

        const peer = 'peer0.org1.example.com';

        it('should return them when provided', async () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: JSON.parse(connectionProfile.toString()),
                discover: true
            });

            connectionProfileDefinition.getGrpcOptionForPeer(peer).should.deep.equal(
                {
                    'ssl-target-name-override': 'peer0.org1.example.com',
                    'hostnameOverride': 'peer0.org1.example.com'
                }
            );
        });

        it('should through an error if peer specified was not provided in the connection Profile', async () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: noPeerConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getGrpcOptionForPeer(peer);
            }).should.throw(
                `${peer} provided is not present in the connection profile`
            );
        });

        it('should through an error if no peers property is found', async () => {
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: blankConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getGrpcOptionForPeer(peer);
            }).should.throw(
                'No peers property can be found in the connection profile provided'
            );
        });

        it('should through an error if no grpcOption property for the provided peer is found', async () => {
            const noGrpcOptionsConnectionProfile = {
                peers: {
                    'peer0.org1.example.com': {
                    }
                }
            };
            const connectionProfileDefinition = new ConnectionProfileDefinition(mspId, {
                loadedConnectionProfile: noGrpcOptionsConnectionProfile,
                discover: false
            });
            (() => {
                connectionProfileDefinition.getGrpcOptionForPeer(peer);
            }).should.throw(
                `no grpcs option fop ${peer} were provided in the connection profile`
            );
        });
    });
});
