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
const should = chai.should();
const mockery = require('mockery');
const path = require('path');

const DefaultEventHandlerStrategies = {};
const DefaultQueryHandlerStrategies = {};

const configWith2Orgs1AdminInWallet = '../../sample-configs/BasicConfig.yaml';
const configWith2Orgs1AdminInWalletNotMutual = '../../sample-configs/BasicConfigNotMutual.yaml';

const { grpc, connect, crypto, signers, Gateway, Transaction, Network, Wallets } = require('./PeerGatewayStubs');
//const GenerateConfiguration = require('../../utils/GenerateConfiguration');
//const { createPrivateKey } = require('crypto');
const ConnectorConfigurationFactory = require('../../../lib/connector-configuration/ConnectorConfigurationFactory');


describe('A Fabric-Gateway sdk gateway', () => {

    let PeerGateway;
    let GenerateWallet;
    let FabricConnectorContext;
    let TxStatus;

    before(() => {
        mockery.enable({
            warnOnReplace: false,
            warnOnUnregistered: false,
            useCleanCache: true
        });

        mockery.registerMock('fabric-gateway', {
            DefaultEventHandlerStrategies,
            DefaultQueryHandlerStrategies,
            Wallets,
            signers,
            connect
        });

        mockery.registerMock('fabric-gateway/package', {version: '1.0.1'});

        mockery.registerMock('crypto', crypto);

        mockery.registerMock('@grpc/grpc-js', grpc);

        PeerGateway = require('../../../lib/connector-versions/peer-gateway/PeerGateway');

        GenerateWallet = require('../../utils/GenerateWallet');
        FabricConnectorContext = require('../../../lib/FabricConnectorContext');
        TxStatus = require('@hyperledger/caliper-core').TxStatus;

    });

    after(() => {
        mockery.deregisterAll();
        mockery.disable();
    });

    let walletFacadeFactory;

    beforeEach(() => {
        Gateway.reset();
        const walletSetup = new GenerateWallet().createStandardTestWalletSetup();
        walletFacadeFactory = walletSetup.walletFacadeFactory;
    });

    it('should be able to initialise in preperation for use by a caliper master', async () => {
        const connectorConfiguration = await new ConnectorConfigurationFactory().create(path.resolve(__dirname, configWith2Orgs1AdminInWallet), walletFacadeFactory);
        const fabricGateway = new PeerGateway(connectorConfiguration, 1, 'fabric');
        await fabricGateway.init().should.not.be.rejected;
    });

    it('should be able to initialise in preperation for use by a caliper master when mutual tls is false', async () => {
        const connectorConfiguration = await new ConnectorConfigurationFactory().create(path.resolve(__dirname, configWith2Orgs1AdminInWalletNotMutual), walletFacadeFactory);
        const fabricGateway = new PeerGateway(connectorConfiguration, 1, 'fabric');
        await fabricGateway.init().should.not.be.rejected;
    });

    it('should do nothing when attempting to install a smart contract', async () => {
        const connectorConfiguration = await new ConnectorConfigurationFactory().create(path.resolve(__dirname, configWith2Orgs1AdminInWallet), walletFacadeFactory);
        const fabricGateway = new PeerGateway(connectorConfiguration, 1, 'fabric');
        await fabricGateway.installSmartContract().should.not.be.rejected;
    });

    it('should return a context when preparing for use by a caliper worker', async () => {
        const connectorConfiguration = await new ConnectorConfigurationFactory().create(path.resolve(__dirname, configWith2Orgs1AdminInWallet), walletFacadeFactory);
        const fabricGateway = new PeerGateway(connectorConfiguration, 1, 'fabric');
        const context = await fabricGateway.getContext();
        context.should.be.instanceOf(FabricConnectorContext);
    });

    it('should return the same context when requested multiple times', async () => {
        const connectorConfiguration = await new ConnectorConfigurationFactory().create(path.resolve(__dirname, configWith2Orgs1AdminInWallet), walletFacadeFactory);
        const fabricGateway = new PeerGateway(connectorConfiguration, 1, 'fabric');
        const context = await fabricGateway.getContext();
        const context2 = await fabricGateway.getContext();
        context2.should.equal(context);
    });

    it('should create one Gateway for organization', async () => {
        const connectorConfiguration = await new ConnectorConfigurationFactory().create(path.resolve(__dirname, configWith2Orgs1AdminInWallet), walletFacadeFactory);
        const fabricGateway = new PeerGateway(connectorConfiguration, 1, 'fabric');
        const context = await fabricGateway.getContext();
        context.should.be.instanceOf(FabricConnectorContext);
        Gateway.constructed.should.equal(2);
    });

    it('should disconnect Gateways when a context is released', async () => {
        const connectorConfiguration = await new ConnectorConfigurationFactory().create(path.resolve(__dirname, configWith2Orgs1AdminInWallet), walletFacadeFactory);
        const fabricGateway = new PeerGateway(connectorConfiguration, 1, 'fabric');
        await fabricGateway.getContext();
        await fabricGateway.releaseContext();
        Gateway.closed.should.equal(2);
    });

    describe('when submitting a request to fabric', () => {
        let fabricGateway;

        beforeEach(async () => {
            Transaction.reset();
            Network.reset();
            const connectorConfiguration = await new ConnectorConfigurationFactory().create(path.resolve(__dirname, configWith2Orgs1AdminInWallet), walletFacadeFactory);
            fabricGateway = new PeerGateway(connectorConfiguration, 1, 'fabric');
            await fabricGateway.getContext();
        });

        afterEach(async () => {
            await fabricGateway.releaseContext();
        });

        describe('should throw an error', () => {
            it('when invokerIdentity is not known', async () => {
                const request = {
                    channel: 'mychannel',
                    contractId: 'marbles',
                    contractFunction: 'myFunction',
                    invokerIdentity: 'NoOne',
                };
                await fabricGateway._sendSingleRequest(request).should.be.rejectedWith(/No contracts for invokerIdentity NoOne found/);
            });

            it('when invokerMspId is not known', async () => {
                const request = {
                    channel: 'mychannel',
                    contractId: 'marbles',
                    contractFunction: 'myFunction',
                    invokerMspId: 'Org7',
                    invokerIdentity: 'admin'
                };
                await fabricGateway._sendSingleRequest(request).should.be.rejectedWith(/No contracts for invokerIdentity admin in Org7 found/);
            });

            it('when contractFunction not provided', async () => {
                const request = {
                    channel: 'mychannel',
                    contractId: 'marbles',
                    contractFunction: '',
                    invokerIdentity: 'admin'
                };
                await fabricGateway._sendSingleRequest(request).should.be.rejectedWith(/No contractFunction provided/);
                request.contractFunction = null;
                await fabricGateway._sendSingleRequest(request).should.be.rejectedWith(/No contractFunction provided/);
                delete request.contractFunction;
                await fabricGateway._sendSingleRequest(request).should.be.rejectedWith(/No contractFunction provided/);
            });

            it('when no contractId provided', async () => {
                const request = {
                    channel: 'mychannel',
                    contractId: '',
                    contractFunction: '',
                    invokerIdentity: 'admin'
                };
                await fabricGateway._sendSingleRequest(request).should.be.rejectedWith(/No contractId provided/);
                request.contractId = null;
                await fabricGateway._sendSingleRequest(request).should.be.rejectedWith(/No contractId provided/);
                delete request.contractId;
                await fabricGateway._sendSingleRequest(request).should.be.rejectedWith(/No contractId provided/);
            });

            it('when channel provided but contractId is not a valid chaincode id', async () => {
                const request = {
                    channel: 'yourchannel',
                    contractId: 'findingMyMarbles',
                    contractFunction: 'myFunction',
                    invokerIdentity: 'admin',
                };
                await fabricGateway._sendSingleRequest(request).should.be.rejectedWith(/Unable to find specified contract findingMyMarbles on channel yourchannel/);
            });

            it('when no channel provided and contract id is not a valid contract id', async () => {
                const request = {
                    channel: '',
                    contractId: 'not-a-valid-contract-id',
                    contractFunction: '',
                    invokerIdentity: 'admin',
                };
                await fabricGateway._sendSingleRequest(request).should.be.rejectedWith(/Could not find details for contract ID not-a-valid-contract-id/);
            });
        });

        describe('when making a submit request', () => {
            it('should succeed and return with an appropriate TxStatus', async () => {
                const args = ['arg1', 'arg2'];
                const request = {
                    channel: 'mychannel',
                    contractId: 'marbles',
                    contractFunction: 'myFunction',
                    contractArguments: args,
                    invokerIdentity: 'admin',
                };
                const txStatus = await fabricGateway._sendSingleRequest(request);
                txStatus.should.be.instanceOf(TxStatus);
                txStatus.GetID().should.equal('1');
                txStatus.GetStatus().should.equal('success');
                txStatus.GetResult().should.equal('submitResponse');
                txStatus.IsVerified().should.be.true;

                Transaction.submit.should.be.true;
                Transaction.submitArgs.should.deep.equal(args);
                Transaction.constructorArgs.should.equal('myFunction');
                Transaction.reset();

                request.readOnly = false;
                await fabricGateway._sendSingleRequest(request);
                Transaction.submit.should.be.true;
                Transaction.submitArgs.should.deep.equal(args);
                Transaction.constructorArgs.should.equal('myFunction');
            });

            it('should set the transientMap', async () => {
                const request = {
                    channel: 'mychannel',
                    contractId: 'marbles',
                    contractFunction: 'myFunction',
                    transientMap: {'param1': 'value1', 'param2': 'value2'},
                    invokerIdentity: 'admin'
                };
                await fabricGateway._sendSingleRequest(request);
                Transaction.submit.should.be.true;
                Transaction.submitArgs.should.deep.equal([]);
                Transaction.transient.should.deep.equal({
                    'param1': Buffer.from('value1'),
                    'param2': Buffer.from('value2')
                });
                Transaction.constructorArgs.should.equal('myFunction');
            });

            it('should look up the channel and chaincode id from contractId when no channel provided', async () => {
                const request = {
                    contractId: 'lostMyMarbles',
                    contractFunction: 'myFunction',
                    contractArguments: ['arg1'],
                    invokerIdentity: 'admin'
                };
                await fabricGateway._sendSingleRequest(request);
                Gateway.channel.should.equal('yourchannel');
                Network.getContractArgs.should.equal('marbles');
                Transaction.submit.should.be.true;
                Transaction.submitArgs.should.deep.equal(['arg1']);
                Transaction.constructorArgs.should.equal('myFunction');
            });

            it('should return an appropriate TxStatus if submit throws an error', async () => {
                const args = ['arg1', 'arg2'];
                const request = {
                    channel: 'mychannel',
                    contractId: 'marbles',
                    contractFunction: 'myFunction',
                    contractArguments: args,
                    invokerIdentity: 'admin',
                };
                Transaction.throwOnCall(new Error('submit-failure'));
                const txStatus = await fabricGateway._sendSingleRequest(request);
                txStatus.should.be.instanceOf(TxStatus);
                txStatus.GetStatus().should.equal('failed');
                txStatus.GetResult().should.equal('');
                txStatus.IsVerified().should.be.true;
            });

            it('should succeed when no invokerIdentity provided', async () => {
                const request = {
                    channel: 'mychannel',
                    contractId: 'marbles',
                    contractFunction: 'myFunction',
                    invokerIdentity: ''
                };
                let txStatus = await fabricGateway._sendSingleRequest(request);
                txStatus.should.be.instanceOf(TxStatus);
                txStatus.GetID().should.equal('1');
                txStatus.GetStatus().should.equal('success');
                txStatus.GetResult().should.equal('submitResponse');
                txStatus.IsVerified().should.be.true;

                request.invokerIdentity = null;
                txStatus = await fabricGateway._sendSingleRequest(request);
                txStatus.should.be.instanceOf(TxStatus);

                delete request.invokerIdentity;
                txStatus = await fabricGateway._sendSingleRequest(request);
                txStatus.should.be.instanceOf(TxStatus);
            });
        });

        describe('when making an evaluate request', () => {
            it('should succeed and return with an appropriate TxStatus', async () => {
                const args = ['arg1', 'arg2'];
                const request = {
                    channel: 'mychannel',
                    contractId: 'marbles',
                    contractFunction: 'myFunction',
                    contractArguments: args,
                    invokerIdentity: 'admin',
                    readOnly: true
                };
                const txStatus = await fabricGateway._sendSingleRequest(request);
                txStatus.GetID().should.equal('1');
                txStatus.GetStatus().should.equal('success');
                txStatus.GetResult().should.equal('evaluateResponse');
                txStatus.IsVerified().should.be.true;

                Transaction.evaluate.should.be.true;
                Transaction.evaluateArgs.should.deep.equal(args);
                Transaction.constructorArgs.should.equal('myFunction');
            });

            it('should ignore peer targeting', async () => {
                const request = {
                    contractId: 'lostMyMarbles',
                    contractFunction: 'myFunction',
                    contractArguments: ['arg1'],
                    invokerIdentity: 'admin',
                    targetPeers: ['peer1', 'peer3', 'peer4'],
                    readOnly: true
                };
                await fabricGateway._sendSingleRequest(request);
                Transaction.evaluate.should.be.true;
                should.equal(Transaction.endorsingPeers, undefined);
            });

            it('should ignore target organisations', async () => {
                const targetOrganizations = ['Org1MSP', 'Org3MSP'];
                const request = {
                    contractId: 'lostMyMarbles',
                    contractFunction: 'myFunction',
                    contractArguments: ['arg1'],
                    invokerIdentity: 'admin',
                    targetOrganizations,
                    readOnly: true
                };
                await fabricGateway._sendSingleRequest(request);
                Transaction.evaluate.should.be.true;
                should.equal(Transaction.endorsingOrganizations, undefined);
            });

            it('should return an appropriate TxStatus if evaluate throws an error', async () => {
                const args = ['arg1', 'arg2'];
                const request = {
                    channel: 'mychannel',
                    contractId: 'marbles',
                    contractFunction: 'myFunction',
                    contractArguments: args,
                    invokerIdentity: 'admin',
                    readOnly: true
                };
                Transaction.throwOnCall(new Error('submit-failure'));
                const txStatus = await fabricGateway._sendSingleRequest(request);
                txStatus.should.be.instanceOf(TxStatus);
                txStatus.GetStatus().should.equal('failed');
                txStatus.GetResult().should.equal('');
                txStatus.IsVerified().should.be.true;
            });
        });
    });
});
