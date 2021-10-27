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

const grpc = require('@grpc/grpc-js');
const crypto = require('crypto');
const { connect, signers } = require('fabric-gateway');
const { ConnectorBase, CaliperUtils, TxStatus, Version, ConfigUtil } = require('@hyperledger/caliper-core');
const FabricConnectorContext = require('../../FabricConnectorContext');

const logger = CaliperUtils.getLogger('connectors/v2/FabricGateway');

//////////////////////
// TYPE DEFINITIONS //
//////////////////////

/**
 * @typedef {Object} FabricRequestSettings
 *
 * @property {string} channel Required. The name of the channel whose contract should be invoked.
 * @property {string} contractId Required. The name/ID of the contract whose function
 *           should be invoked.
 * @property {string} contractFunction Required. The name of the function that should be
 *           invoked in the contract.
 * @property {string[]} [contractArguments] Optional. The list of {string} arguments that should
 *           be passed to the contract.
 * @property {Map<string, Buffer>} [transientMap] Optional. The transient map that should be
 *           passed to the contract.
 * @property {string} [invokerMspId] Optional. The MspId of the invoker. Required if there are more than
 *           1 organisation defined in the network configuration file
 * @property {string} invokerIdentity Required. The identity name of the invoker
 * @property {boolean} [readOnly] Optional. Indicates whether the request is a submit or evaluation.
 *           contract. If an admin is needed, use the organization name prefixed with a # symbol.
 * @property {string[]} [targetPeers] Optional. An array of endorsing
 *           peer names as the targets of the invoke. When this
 *           parameter is omitted the target list will include the endorsing peers assigned
 *           to the target contract, or if it is also omitted, to the channel.
 * @property {string[]} [targetOrganizations] Optional. An array of endorsing
 *           organizations as the targets of the invoke. If both targetPeers and targetOrganizations
 *           are specified then targetPeers will take precedence
 */

/////////////////////////////
// END OF TYPE DEFINITIONS //
/////////////////////////////

/**
 */
class V2FabricGateway extends ConnectorBase {

    /**
     * Initializes the Fabric adapter.
     * @param {connectorConfiguration} connectorConfiguration the Connector Configuration
     * @param {number} workerIndex the worker index
     * @param {string} bcType The target SUT type
     */
    constructor(connectorConfiguration, workerIndex, bcType) {
        super(workerIndex, bcType);
        this.connectorConfiguration = connectorConfiguration;

        this.fabricNetworkVersion = new Version(require('fabric-gateway/package').version);

        this.contractInstancesByIdentity = new Map();
        this.gatewayInstanceByIdentity = new Map();
        this.peerNameToPeerObjectCache = new Map();
        this.clients = new Map();
        this.context = undefined;

        // Timeouts
        this.configSmallestTimeout = 1000;
        this.configDefaultTimeout = ConfigUtil.get(ConfigUtil.keys.Fabric.Timeout.InvokeOrQuery, 60000);
        this.configCountQueryAsLoad = ConfigUtil.get(ConfigUtil.keys.Fabric.CountQueryAsLoad, true);

        // Gateway connector
        this.configLocalHost = ConfigUtil.get(ConfigUtil.keys.Fabric.Gateway.LocalHost, true);
        this.configEventStrategy = ConfigUtil.get(ConfigUtil.keys.Fabric.Gateway.EventStrategy, 'msp_all');
        this.configQueryStrategy = ConfigUtil.get(ConfigUtil.keys.Fabric.Gateway.QueryStrategy, 'msp_single');
    }

    //////////////////////////
    // PUBLIC API FUNCTIONS //
    //////////////////////////

    /**
     * Prepares the adapter for use by a worker.
     *
     * @param {Number} roundIndex The zero-based round index of the test.
     * @param {Array<string>} args Unused.
     * @return {Promise<FabricConnectorContext>} Returns the unique context for the fabric connector
     * @async
     */
    async getContext(roundIndex, args) {
        if (!this.context) {
            this.context = new FabricConnectorContext(this.workerIndex);
            await this._prepareGatewayAndContractMapsForEachIdentity();
            //await this._buildPeerCache(); // TODO: might be able to do this just once
        }

        return this.context;
    }

    /**
     * Initializes the Fabric adapter for use by the Caliper Master
     * @async
     */
    async init() {
        // Seems to be only for operational initialisation but need to implement as the master
        // will call it
        const defaultOrganization = this.connectorConfiguration.getOrganizations()[0];
        const tlsInfo = this.connectorConfiguration.isMutualTLS() ? 'mutual'
            : (this.connectorConfiguration.getConnectionProfileDefinitionForOrganization(defaultOrganization).isTLSEnabled() ? 'server' : 'none');
        logger.info(`Fabric SDK version: ${this.fabricNetworkVersion.toString()}; TLS based on ${defaultOrganization}: ${tlsInfo}`);
    }

    /**
     * Installs and initializes the specified contracts.
     * @async
     */
    async installSmartContract() {
        logger.warn(`Install smart contract not available with Fabric SDK version: ${this.fabricNetworkVersion.toString()}`);
    }

    /**
     * Send a single request to the backing SUT.
     * @param {FabricRequestSettings} request The request object.
     */
    async _sendSingleRequest(request) {
        if (!request.contractId) {
            throw new Error('No contractId provided in the request');
        }

        if (!request.channel) {
            const contractDetails = this.connectorConfiguration.getContractDetailsForContractId(request.contractId);
            if (!contractDetails) {
                throw new Error(`Could not find details for contract ID ${request.contractId}`);
            }
            request.channel = contractDetails.channel;
            request.contractId = contractDetails.id;
        }

        if (!request.contractFunction) {
            throw new Error('No contractFunction provided in the request');
        }

        if (!request.contractArguments) {
            request.contractArguments = [];
        }

        return await this._submitOrEvaluateTransaction(request, request.readOnly === undefined || !request.readOnly);
    }

    /**
     * Releases the resources of the adapter.
     *
     * @async
     */
    async releaseContext() {
        for (const userName of this.gatewayInstanceByIdentity.keys()) {
            const gateway = this.gatewayInstanceByIdentity.get(userName);
            logger.info(`disconnecting gateway for user ${userName}`);
            gateway.close();
        }
        for (const clientName of this.clients.keys()) {
            const client = this.clients.get(clientName);
            logger.info(`disconnecting gRpc client at peer ${clientName}`);
            client.close();
        }

        this.peerNameToPeerObjectCache.clear();
        this.context = undefined;
    }


    ////////////////////////////////
    // INTERNAL UTILITY FUNCTIONS //
    ////////////////////////////////

    /**
     * Extract and persist Contracts from Gateway Networks for identities listed within the wallet
     * @async
     */
    async _prepareGatewayAndContractMapsForEachIdentity() {
        logger.debug('Entering _prepareGatewayAndContractMapsForEachIdentity');
        for (const organization of this.connectorConfiguration.getOrganizations()) {
            const connectionProfileDefinition = await this.connectorConfiguration.getConnectionProfileDefinitionForOrganization(organization);
            const peers = await connectionProfileDefinition.getPeers();
            const aliasNames = await this.connectorConfiguration.getAliasNamesForOrganization(organization);
            const walletWithIdentities = this.connectorConfiguration.getWallet();

            for (let i = 0; i < aliasNames.length; i++) {
                const gateway = await this._createGatewayWithIdentity(organization, aliasNames[i], walletWithIdentities, peers[i % peers.length]);
                this.gatewayInstanceByIdentity.set(aliasNames[i], gateway);

                const contractMap = this._createChannelAndChaincodeIdToContractMap(gateway, aliasNames[i]);
                this.contractInstancesByIdentity.set(aliasNames[i], contractMap);
            }
        }
        logger.debug('Exiting _prepareGatewayAndContractMapsForEachIdentity');
    }

    /**
     * Create a map with key of channel+chaincode id to fabric-network contract instances
     * @param {Gateway} gateway the gateway to use
     * @param {string} aliasName, the aliasName of the identity being used by the gateway
     * @returns {Promise<Map<Contract>>} A map of all Contract instances for that identity across all the channels and chaincodes
     * @async
     */
    _createChannelAndChaincodeIdToContractMap(gateway, aliasName) {
        logger.debug('Entering _createChannelAndChaincodeIdToContractMap');
        logger.info(`Generating contract map for user ${aliasName}`);

        const contractMap = new Map();
        const channels = this.connectorConfiguration.getAllChannelNames();
        for (const channel of channels) {

            let network;
            try {
                network = gateway.getNetwork(channel);
            } catch (err) {
                logger.warn(`Couldn't initialize ${channel} for ${aliasName}. ${aliasName} not available for use on this channel. Error: ${err.message}`);
                continue;
            }

            const contracts = this.connectorConfiguration.getContractDefinitionsForChannelName(channel);

            for (const contract of contracts) {
                const networkContract = network.getContract(contract.id);
                contractMap.set(`${channel}_${contract.id}`, networkContract);
            }
        }
        logger.debug('Exiting _createChannelAndChaincodeIdToContractMap');

        return contractMap;
    }

    /**
     * Create a fabric-network gateway instance associated with a specific identity
     * @param {string} mspId The msp id of the organisation which owns the identity
     * @param {string} aliasName The alias name that represents the identity to use
     * @param {*} wallet, the wallet that holds the identity to be used
     * @returns {Promise<Gateway>} a gateway object for the passed user identity
     * @async
     */
    async _createGatewayWithIdentity(mspId, aliasName, wallet, peer) {
        logger.debug(`Entering _createGatewayWithIdentity for alias name ${aliasName}`);
        //create identity from mspId and certificate
        const walletIdentity = await wallet.get(aliasName);
        const cert = walletIdentity.credentials.certificate;
        const identity = { mspId: walletIdentity.mspId, credentials: Buffer.from(cert) };

        //create gRpc client to designated port with the tlsCredentials of the peer
        //use one already created if a client for each peer is already created
        let client;
        if (peer in this.clients.keys()) {
            client = this.clients.get(peer);
        } else {
            const connectionProfileDefinition = await this.connectorConfiguration.getConnectionProfileDefinitionForOrganization(mspId);
            const tlsRootCert = await connectionProfileDefinition.getTlsCertForPeer(peer);
            const tlsCredentials = grpc.credentials.createSsl(Buffer.from(tlsRootCert));
            const peerEndpointUrl = await connectionProfileDefinition.getEndPointForPeer(peer);
            const peerEndpoint = peerEndpointUrl.toString().replace('grpcs://', '');
            const grpcOptions = {};//await connectionProfileDefinition.getGrpcOptionForPeer(peer);
            grpcOptions['grpc.ssl_target_name_override'] = 'peer0.org1.example.com';
            grpcOptions['grpc.hostnameOverride'] = 'peer0.org1.example.com';
	        grpcOptions['grpc.max_receive_message_length'] = -1;
	        grpcOptions['grpc.max_send_message_length'] = -1;
            const GrpcClient = grpc.makeGenericClientConstructor({}, '');
	        client = new GrpcClient(peerEndpoint, tlsCredentials, grpcOptions);
            this.clients.set(peer, client);
        }
        //create signer using the private key of the peer
        const privateKeyPem = walletIdentity.credentials.privateKey;
        const privateKey = crypto.createPrivateKey(privateKeyPem);
        const signer = signers.newPrivateKeySigner(privateKey);

        const gateway = connect({ client, identity, signer });

        logger.debug('Exiting _createGatewayWithIdentity');

        return gateway;
    }

    /**
     */
    async _buildPeerCache() {
        logger.debug('Entering _buildPeerCache');
        for (const aliasName of this.gatewayInstanceByIdentity.keys()) {
            const gateway = this.gatewayInstanceByIdentity.get(aliasName);
            const channelNames = this.connectorConfiguration.getAllChannelNames();

            for (const channelName of channelNames) {
                const channel = await gateway.getNetwork(channelName);

                // WARNING: This uses an internal API to get the endorsers
                for (const peerObject of channel.client.getEndorsers()) {
                    this.peerNameToPeerObjectCache.set(peerObject.name, peerObject);
                }
            }
        }
        logger.debug('Exiting _buildPeerCache');
    }

    /**
     * Submit or evaluate a transaction
     * @param {FabricRequestSettings} invokeSettings The settings associated with the transaction submission.
     * @param {boolean} isSubmit boolean flag to indicate if the transaction is a submit or evaluate
     * @return {Promise<TxStatus>} The result and stats of the transaction invocation.
     * @async
     */
    async _submitOrEvaluateTransaction(invokeSettings, isSubmit) {
        const smartContract = await this._getContractForIdentityOnChannelWithChaincodeID(invokeSettings.invokerMspId, invokeSettings.invokerIdentity, invokeSettings.channel, invokeSettings.contractId);

        // Build the Caliper TxStatus, this is a reduced item when compared to the low level API capabilities
        // - TxID is not available until after transaction submit/evaluate and must be set at that point
        const invokeStatus = new TxStatus();

        //set the proposal Options(arguments, endorsingOrganizations, transientData)
        const proposalOptions = {};
        //add contract arguments to proposal Options
        proposalOptions["arguments"] = invokeSettings.contractArguments;
        // Add transient data if present
        if (invokeSettings.transientMap) {
            const transientData = {};
            const keys = Array.from(Object.keys(invokeSettings.transientMap));
            keys.forEach((key) => {
                transientData[key] = Buffer.from(invokeSettings.transientMap[key]);
            });
            proposalOptions["transientData"] = transientData;
        }
        // Add endorsing organizations if present if present
        // not relevant because not possible to target specific peers using the new fabric-gateway
        /*if (invokeSettings.targetPeers && isSubmit) {
            if (Array.isArray(invokeSettings.targetPeers) && invokeSettings.targetPeers.length > 0) {
                const targetPeerObjects = [];
                for (const name of invokeSettings.targetPeers) {
                    const peer = this.peerNameToPeerObjectCache.get(name);
                    if (peer) {
                        targetPeerObjects.push(peer);
                    }
                }
                if (targetPeerObjects.length > 0) {
                    proposalOptions.set("endorsingOrganization", targetPeerObjects);
                }
            } else {
                logger.warn(`${invokeSettings.targetPeers} is not a populated array, no peers targeted`);
            }
        } else if (invokeSettings.targetOrganizations && isSubmit) {
            if (Array.isArray(invokeSettings.targetOrganizations) && invokeSettings.targetOrganizations.length > 0) {
                proposalOptions.set("endorsingOrganization", invokeSettings.targetOrganizations)
            } else {
                logger.warn(`${invokeSettings.targetOrganizations} is not a populated array, no orgs targeted`);
            }
        }*/
        let transaction;
        try {
            if (isSubmit) {
                invokeStatus.Set('request_type', 'transaction');
                invokeStatus.Set('time_create', Date.now());
                transaction = await smartContract.submitAsync(invokeSettings.contractFunction, proposalOptions);
                const status = await transaction.getStatus();
                invokeStatus.SetStatusSuccess();
                invokeStatus.SetResult(transaction.getResult());
            } else {
                if (invokeSettings.targetPeers || invokeSettings.targetOrganizations) {
                    logger.warn('targetPeers or targetOrganizations options are not valid for query requests');
                }
                transaction = await smartContract.newProposal(invokeSettings.contractFunction, proposalOptions);

                invokeStatus.Set('request_type', 'query');
                invokeStatus.Set('time_create', Date.now());

                invokeStatus.SetResult(await transaction.evaluate());
                invokeStatus.SetStatusSuccess();
            }

            invokeStatus.SetVerification(true);
            invokeStatus.SetID(transaction.getTransactionId());

            return invokeStatus;
        } catch (err) {
            logger.error(`Failed to perform ${isSubmit ? 'submit' : 'query'} transaction [${invokeSettings.contractFunction}] using arguments [${invokeSettings.contractArguments}],  with error: ${err.stack ? err.stack : err}`);
            invokeStatus.SetStatusFail();
            invokeStatus.SetVerification(true);
            invokeStatus.SetResult('');
            return invokeStatus;
        }

    }

    /**
     * Get the the specific contract for the identity invoking the request
     * @param {string} mspId the mspId of the organisation that owns the identity
     * @param {string} identityName the identity requested to be used by the workload
     * @param {string} channelName the channel name the contract exists on
     * @param {string} contractId the name of the contract to return
     * @returns {Promise<FabricNetworkAPI.Contract>} A contract that may be used to submit or evaluate transactions
     * @async
     */
    async _getContractForIdentityOnChannelWithChaincodeID(mspId, identityName, channelName, contractId) {
        logger.debug('Entering _getContractForIdentityOnChannelWithChaincodeID');

        const aliasName = this.connectorConfiguration.getAliasNameForOrganizationAndIdentityName(mspId, identityName);
        const contractSet = this.contractInstancesByIdentity.get(aliasName);

        // If no contract set found, there is a user configuration/test specification error, so it should terminate
        if (!contractSet) {
            throw Error(`No contracts for invokerIdentity ${identityName}${mspId ? ` in ${mspId}` : ''} found. Identity and/or MspId does not exist`);
        }

        const contract = contractSet.get(`${channelName}_${contractId}`);

        // If no contract found, there is a user configuration/test specification error, so it should terminate
        if (!contract) {
            throw Error(`Unable to find specified contract ${contractId} on channel ${channelName}!`);
        }

        logger.debug('Exiting _getContractForIdentityOnChannelWithChaincodeID');

        return contract;
    }
}

module.exports = V2FabricGateway;
